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
        logging.FileHandler('get_today_picks.log')
    ]
)
logger = logging.getLogger(__name__)

def get_today_giants_picks(today_date: str = None) -> dict:
    """
    오늘 날짜의 거장별 선정 종목을 today_{거장이름}_pick_{n} 형식의 변수로 반환하고,
    src_data/todays_picks.feather에 날짜별, 거장별 선정 종목을 저장.

    Args:
        today_date (str, optional): 대상 날짜 (형식: 'YYYY-MM-DD'). 기본값은 오늘.

    Returns:
        dict: {변수명: 티커} 형식의 딕셔너리 (예: {'today_benjamin_graham_pick_1': '035420.KQ'})
    """
    # 오늘 날짜 설정
    if today_date is None:
        today_date = datetime.now().strftime('%Y-%m-%d')
    try:
        today_date = pd.to_datetime(today_date).strftime('%Y-%m-%d')
    except ValueError:
        logger.error(f"잘못된 날짜 형식: {today_date}. 'YYYY-MM-DD' 형식이어야 합니다.")
        return {}

    # 경로 설정
    base_dir = Path(__file__).parent.parent / "src_data"
    picks_dir = base_dir / "giants_pick"
    output_file = base_dir / "todays_picks" / "todays_picks.feather"
    
    # 디렉토리가 없으면 생성
    picks_dir.mkdir(parents=True, exist_ok=True)

    if not picks_dir.exists():
        logger.error(f"giants_pick 디렉토리가 없습니다: {picks_dir}")
        return {}

    # 거장 목록
    giants = {
        'benjamin_graham': 'Graham_Pick',
        'ken_fisher': 'Fisher_Pick',
        'peter_lynch': 'Lynch_Pick',
        'jesse_livermore': 'Livermore_Pick',
        'mark_minervini': 'Minervini_Pick',
        'william_oneil': 'ONeil_Pick'
    }

    # 결과 저장용 딕셔너리
    result = {}
    # feather 파일에 저장할 데이터
    picks_data = []

    # 각 거장별 선정 종목 추출
    for giant_name, column in giants.items():
        picks = []
        # 모든 feather 파일 순회
        for file_path in picks_dir.glob('*_giants_picks.feather'):
            try:
                df = pd.read_feather(file_path)
                df['Date'] = pd.to_datetime(df['Date'])
                
                # 오늘 날짜 데이터 필터링
                df_today = df[df['Date'] == today_date]
                if not df_today.empty and column in df_today.columns and df_today[column].iloc[0]:
                    ticker = df_today['Ticker'].iloc[0]
                    picks.append(ticker)
                    # feather 파일용 데이터 추가
                    picks_data.append({
                        'Date': today_date,
                        'Giant': giant_name.replace('_', ' ').title(),
                        'Ticker': ticker
                    })
            except Exception as e:
                logger.warning(f"파일 {file_path} 처리 중 오류: {e}")
                continue
        
        # 선정 종목을 변수명으로 매핑
        for i, ticker in enumerate(picks, 1):
            var_name = f"today_{giant_name}_pick_{i}"
            result[var_name] = ticker
        
        logger.info(f"{giant_name.replace('_', ' ').title()}: {len(picks)}개 종목 선정")

    # feather 파일 저장
    if picks_data:
        picks_df = pd.DataFrame(picks_data)
        # 기존 데이터 로드 (있으면)
        try:
            existing_df = pd.read_feather(output_file)
            combined_df = pd.concat([existing_df, picks_df]).drop_duplicates(subset=['Date', 'Giant', 'Ticker'])
        except FileNotFoundError:
            combined_df = picks_df
        except Exception as e:
            logger.error(f"todays_picks.feather 읽기/쓰기 중 오류: {e}")
            combined_df = picks_df
        
        # 데이터 저장
        combined_df.reset_index(drop=True).to_feather(output_file)
        logger.info(f"{today_date} 데이터가 {output_file}에 저장되었습니다.")
    else:
        logger.warning(f"{today_date}에 선정된 종목이 없습니다.")

    return result

if __name__ == "__main__":
    # 테스트 실행
    picks = get_today_giants_picks('2025-05-29')
    for var_name, ticker in picks.items():
        print(f"{var_name} = {ticker}")