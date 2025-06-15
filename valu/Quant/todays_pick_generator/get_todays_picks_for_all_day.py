import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('get_all_dates_picks.log')
    ]
)
logger = logging.getLogger(__name__)

def process_giants_picks(picks_dir: Path, output_file: Path) -> dict:
    """
    giants_pick 폴더 내 모든 *_giants_picks.feather 파일을 한 번에 읽어
    날짜별, 거장별 선정 종목을 처리하고 결과를 반환하며, todays_picks.feather에 저장.
    각 거장/날짜당 모든 True 티커를 선택.

    Args:
        picks_dir (Path): giants_pick 디렉토리 경로.
        output_file (Path): 출력 feather 파일 경로.

    Returns:
        dict: {날짜: {변수명: 티커}} 형식의 딕셔너리
    """
    # 거장 목록
    giants = {
        'benjamin_graham': 'Graham_Pick',
        'ken_fisher': 'Fisher_Pick',
        'peter_lynch': 'Lynch_Pick',
        'jesse_livermore': 'Livermore_Pick',
        'mark_minervini': 'Minervini_Pick',
        'william_oneil': 'ONeil_Pick'
    }

    # 모든 feather 파일 읽기
    all_dfs = []
    for file_path in picks_dir.glob('*_giants_picks.feather'):
        try:
            df = pd.read_feather(file_path, columns=['Date', 'Ticker'] + list(giants.values()))
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            # 데이터 타입 점검 및 변환
            for col in giants.values():
                if col in df.columns:
                    unique_vals = df[col].dropna().unique()
                    logger.info(f"{file_path} - {col} 값: {unique_vals}")
                    if df[col].dtype == 'object':
                        df[col] = df[col].replace({'True': True, 'False': False, 'true': True, 'false': False}).astype('boolean')
            all_dfs.append(df)
            logger.info(f"{file_path}에서 {len(df)}개의 행 읽음")
        except Exception as e:
            logger.warning(f"파일 {file_path} 처리 중 오류: {e}")
            continue

    if not all_dfs:
        logger.warning("giants_pick 폴더에서 유효한 데이터를 찾을 수 없습니다.")
        return {}

    # 데이터프레임 병합 및 중복 제거
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['Date', 'Ticker'] + list(giants.values()))
    logger.info(f"병합 후 행 수: {len(combined_df)}")
    logger.info(f"중복 제거 후 행 수: {len(combined_df)}")

    # 결과 딕셔너리 및 출력 데이터 초기화
    all_results = {}
    picks_data = []

    # 날짜별 처리
    for date in sorted(combined_df['Date'].unique()):
        date_df = combined_df[combined_df['Date'] == date]
        date_results = {}

        # 각 거장별로 모든 True 티커 선택
        for giant_name, column in giants.items():
            if column in date_df.columns:
                # 명시적으로 True인 행 선택
                valid_picks = date_df[date_df[column] == True][['Ticker']]
                if not valid_picks.empty:
                    for i, ticker in enumerate(valid_picks['Ticker'], 1):
                        var_name = f"today_{giant_name}_pick_{i}"
                        date_results[var_name] = ticker
                        picks_data.append({
                            'Date': date,
                            'Giant': giant_name.replace('_', ' ').title(),
                            'Ticker': ticker
                        })
                    logger.info(f"{date} - {giant_name.replace('_', ' ').title()}: {len(valid_picks)}개 종목 선정 - {valid_picks['Ticker'].tolist()}")
                else:
                    logger.info(f"{date} - {giant_name.replace('_', ' ').title()}: 종목 선정되지 않음")

        all_results[date] = date_results

    # feather 파일 저장
    if picks_data:
        picks_df = pd.DataFrame(picks_data)
        try:
            existing_df = pd.read_feather(output_file)
            combined_df = pd.concat([existing_df, picks_df]).drop_duplicates(subset=['Date', 'Giant', 'Ticker'])
        except FileNotFoundError:
            combined_df = picks_df
        except Exception as e:
            logger.error(f"todays_picks.feather 읽기/쓰기 중 오류: {e}")
            combined_df = picks_df

        combined_df.reset_index(drop=True).to_feather(output_file)
        logger.info(f"데이터가 {output_file}에 저장되었습니다.")
    else:
        logger.warning("선정된 종목이 없습니다.")

    return all_results

def get_all_dates_giants_picks() -> dict:
    """
    giants_pick 폴더 내 모든 데이터를 처리하여 날짜별, 거장별 선정 종목을 반환.

    Returns:
        dict: {날짜: {변수명: 티커}} 형식의 딕셔너리
    """
    # 경로 설정
    base_dir = Path(__file__).parent.parent / "src_data"
    picks_dir = base_dir / "giants_pick"
    output_file = base_dir / "todays_picks" / "todays_picks.feather"

    # 디렉토리가 없으면 생성
    picks_dir.mkdir(parents=True, exist_ok=True)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if not picks_dir.exists():
        logger.error(f"giants_pick 디렉토리가 없습니다: {picks_dir}")
        return {}

    return process_giants_picks(picks_dir, output_file)

if __name__ == "__main__":
    # 테스트 실행
    all_picks = get_all_dates_giants_picks()
    for date, picks in all_picks.items():
        print(f"\n=== {date} ===")
        for var_name, ticker in picks.items():
            print(f"{var_name} = {ticker}")