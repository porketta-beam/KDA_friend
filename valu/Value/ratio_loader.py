import pandas as pd
import requests
from tqdm import tqdm
import time
import os

def read_ticker_info(filepath: str) -> pd.DataFrame:
    """ticker_info.csv 파일을 읽고 corp_code를 8자리 문자열로 변환"""
    print(f"{filepath} 파일 읽는 중...")
    try:
        df = pd.read_csv(filepath, dtype={'ticker': str, 'corp_code': str})
        df['corp_code'] = df['corp_code'].apply(lambda x: str(x).zfill(8))
        print(f"상장사 수: {len(df)}")
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"{filepath} 파일이 존재하지 않습니다.")
    except Exception as e:
        raise RuntimeError(f"{filepath} 파일 읽기 실패: {e}")

def fetch_financial_indicators(api_key: str, df_corp: pd.DataFrame, bsns_year: str, reprt_code: str = '11011', idx_cl_codes: list = None) -> pd.DataFrame:
    """지정된 재무지표들을 수집하여 DataFrame으로 반환"""

    # 수익성지표 : M210000 안정성지표 : M220000 성장성지표 : M230000 활동성지표 : M240000

    if idx_cl_codes is None:
        idx_cl_codes = ['M210000', 'M220000', 'M230000', 'M240000']
    
    result_list = []
    for idx_cl_code in idx_cl_codes:
        print(f"{idx_cl_code} 재무지표 요청 중...")
        for _, row in tqdm(df_corp.iterrows(), total=len(df_corp), desc=f"{idx_cl_code} 처리"):
            corp_code = row['corp_code']
            corp_name = row['corp_name']
            ticker = row['ticker']

            params = {
                'crtfc_key': api_key,
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': reprt_code,
                'idx_cl_code': idx_cl_code,
            }

            try:
                res = requests.get('https://opendart.fss.or.kr/api/fnlttSinglIndx.json', params=params)
                data = res.json()

                if data['status'] == '000':
                    for item in data.get('list', []):
                        if 'idx_val' in item:
                            result_list.append({
                                'corp_name': corp_name,
                                'corp_code': corp_code,
                                'ticker': ticker,
                                'idx_nm': item['idx_nm'],
                                'idx_val': pd.to_numeric(item['idx_val'], errors='coerce'),
                                'stlm_dt': item['stlm_dt'],
                                'idx_cl_code': idx_cl_code
                            })
                else:
                    print(f"[{corp_name}] 오류: {data['message']}")
                time.sleep(0.3)  # DART API 요청 제한 준수
            except Exception as e:
                print(f"[{corp_name}] 예외 발생: {e}")

    df_result = pd.DataFrame(result_list)
    print("\n✅ 재무지표 수집 완료")
    return df_result

def create_pivot_table(df_result: pd.DataFrame) -> pd.DataFrame:
    """재무지표 데이터를 피벗 테이블로 변환"""
    # NaN 값 제거
    print("\nNaN 값 확인:")
    print(df_result[['corp_code', 'idx_nm', 'idx_val']].isna().sum())
    df_result = df_result.dropna(subset=['corp_code', 'idx_nm', 'idx_val'])

    # corp_code를 8자리 문자열로 보장
    df_result['corp_code'] = df_result['corp_code'].apply(lambda x: str(x).zfill(8))

    # 중복 제거: corp_code와 idx_nm 기준으로 idx_val 최대값 선택
    df_unique = df_result.loc[df_result.groupby(['corp_code', 'idx_nm'])['idx_val'].idxmax()]

    # 피벗 테이블 생성
    pivot_df = df_unique.pivot_table(
        values='idx_val',
        index=['corp_name', 'corp_code', 'ticker'],
        columns='idx_nm',
        aggfunc='first'
    ).reset_index()

    # 컬럼 순서 조정
    columns = ['ticker', 'corp_name', 'corp_code'] + [col for col in pivot_df.columns if col not in ['ticker', 'corp_name', 'corp_code']]
    pivot_df = pivot_df[columns]

    return pivot_df

def save_to_csv(df: pd.DataFrame, filepath: str):
    """DataFrame을 CSV로 저장"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"\n✅ 결과가 {filepath}에 저장되었습니다.")
    except Exception as e:
        raise RuntimeError(f"CSV 저장 실패: {e}")

def main(api_key: str, bsns_year: str, output_dir: str, ticker_info_path: str = 'ticker_info.csv', reprt_code: str = '11011', test_limit: int = None):
    """지정된 사업연도의 모든 재무지표를 수집하여 피벗 테이블로 저장"""

    # 1분기보고서 : 11013, 반기보고서 : 11012, 3분기보고서 : 11014, 사업보고서 : 11011

    # ticker_info.csv 읽기
    df_corp = read_ticker_info(os.path.join(output_dir, ticker_info_path))

    # # 테스트를 위해 기업 수 제한 (실제 사용시에는 삭제하면 됨)
    # if test_limit is not None:
    #     df_corp = df_corp.head(test_limit)
    #     print(f"\n테스트를 위해 처음 {test_limit}개 기업만 처리:")
    #     print(df_corp[['corp_name', 'corp_code', 'ticker']])
    
    # 재무지표 수집
    df_result = fetch_financial_indicators(api_key, df_corp, bsns_year, reprt_code)
    
    # 피벗 테이블 생성
    pivot_df = create_pivot_table(df_result)
    
    # 결과 저장
    save_path = os.path.join(output_dir, f"{bsns_year}_ratios_pivot.csv")
    save_to_csv(pivot_df, save_path)
    
    return pivot_df

if __name__ == "__main__":
    api_key = api_key
    output_dir = r"C:\Users\gkstnrud\Dev\KDA_friend\valu\Value\src_data"
    bsns_year = "2024"
    main(api_key, bsns_year, output_dir, test_limit=20)