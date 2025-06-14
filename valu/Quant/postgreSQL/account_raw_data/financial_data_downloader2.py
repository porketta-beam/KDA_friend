import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
import uuid
from datetime import datetime
from utils import load_env, create_connection, get_company_tickers, get_date_uuids, get_acc_ids, to_numeric_safe
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 🔑 API Key
API_KEY = "58d6b02297b245067c9e7d9f1a32639ca1140072"  # 실제 API 키로 교체

# 보고서 코드
REPORT_CODES = {
    "q1": "11013",
    "semi": "11012",
    "q3": "11014",
    "annual": "11011"
}

FS_PRIORITY = ["CFS", "OFS"]

def get_latest_date(conn, ticker, report_code):
    logger.info("티커 %s, 보고서 %s의 최신 데이터 날짜 조회 시작", ticker, report_code)
    cursor = conn.cursor()
    query = """
    SELECT MAX(d.date)
    FROM account_raw_data ard
    JOIN date d ON ard.date_id = d.date_id
    WHERE ard.ticker = %s AND ard.report_code = %s
    """
    cursor.execute(query, (ticker, report_code))
    result = cursor.fetchone()
    cursor.close()
    if result[0]:
        logger.info("티커 %s, 보고서 %s의 최신 날짜: %s", ticker, report_code, result[0])
        return result[0].year
    logger.info("티커 %s, 보고서 %s에 기존 데이터 없음, 기본 연도 사용", ticker, report_code)
    return 2024

def fetch_financial_data(corp_code, year, reprt_code):
    logger.info("티커 %s, 연도 %s, 보고서 %s 데이터 다운로드 시작", corp_code, year, reprt_code)
    corp_code = str(corp_code).zfill(8)
    for fs_div in FS_PRIORITY:
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key": API_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": reprt_code,
            "fs_div": fs_div
        }
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if data.get("status") == "000" and data.get("list"):
                logger.info("티커 %s, 연도 %s, 보고서 %s, %s 데이터 다운로드 완료: %d 행", corp_code, year, reprt_code, fs_div, len(data["list"]))
                return data["list"]
            elif data.get("status") == "013":
                logger.warning("티커 %s, 연도 %s, 보고서 %s, %s 데이터 없음", corp_code, year, reprt_code, fs_div)
                continue
        except Exception as e:
            logger.error("티커 %s, 연도 %s, 보고서 %s 데이터 다운로드 오류: %s", corp_code, year, reprt_code, e)
    return []

def download_and_reshape_data(ticker, corp_code, corp_name, year, reprt_code):
    logger.info("티커 %s 데이터 처리 시작: 연도 %s, 보고서 %s", ticker, year, reprt_code)
    response = fetch_financial_data(corp_code, year, reprt_code)
    if not response:
        logger.warning("티커 %s에 대한 데이터 없음: 연도 %s, 보고서 %s", ticker, year, reprt_code)
        return None

    # 데이터프레임으로 변환
    data = []
    for item in response:
        row = {
            "date": item.get("thstrm_dt"),  # 재무제표 기준 날짜
            "ticker": ticker,
            "report_code": reprt_code,
            "data_type": item.get("account_nm"),
            "value": to_numeric_safe(item.get("thstrm_amount")),
            "currency": item.get("currency")
        }
        data.append(row)
    
    df = pd.DataFrame(data)
    logger.info("티커 %s 데이터 처리 완료: %d 행", ticker, len(df))
    # 디버깅: 데이터 확인
    if df['value'].isna().all():
        logger.warning("티커 %s의 모든 value 값이 NaN입니다.", ticker)
    return df[['date', 'ticker', 'report_code', 'data_type', 'value', 'currency']]

def map_data_to_ids(data_df, date_map, acc_map):
    logger.info("데이터프레임 ID 매핑 시작")
    try:
        # date 열을 date_id로 변환
        data_df['date_id'] = data_df['date'].map(lambda x: date_map.get(x.replace(".", "-") if isinstance(x, str) else x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x))
        # data_type 열을 acc_id로 변환
        data_df['acc_id'] = data_df['data_type'].map(lambda x: acc_map.get(x))
        # 유효하지 않은 date_id 또는 acc_id는 제외
        original_len = len(data_df)
        data_df = data_df.dropna subset=['date_id', 'acc_id'])
        logger.info(" 데이터프레임 ID 매핑 완료: %d/%d 행 유지", len(data_df), original_len)
        if original_len > len(data_df):
            logger.warning("date_id 또는 acc_id 누락으로 인해 %d 행 제외", original_len - len(data_df))
        return data_df[['date_id', 'ticker', 'report_code', 'acc_id', 'value', 'currency']]
    except Exception as e:
        logger.error("데이터프레임 ID 매핑 오류: %s", e)
        return None

def insert_financial_data(conn, ticker, data_df):
    logger.info("티커 %s 데이터 삽입 시작", ticker)
    cursor = conn.cursor()
    try:
        batch_data = []
        for _, row in data_df.iterrows():
            value = float(row['value']) if pd.notna(row['value']) else None
            batch_data.append((
                str(uuid.uuid4()),  # acc_raw_id
                row['date_id'],
                row['ticker'],
                row['report_code'],
                row['acc_id'],
                value,
                row['currency']
            ))

        if batch_data:
            cursor.executemany(
                """
                INSERT INTO account_raw_data (acc_raw_id, date_id, ticker, report_code, acc_id, acc_raw_data, currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                batch_data
            )
            conn.commit()
            logger.info("티커 %s 데이터 삽입 완료: %d 행", ticker, len(batch_data))
        else:
            logger.warning("티커 %s에 삽입할 데이터 없음", ticker)
    except Exception as e:
        conn.rollback()
        logger.error("티커 %s 데이터 삽입 오류: %s", ticker, e)
    finally:
        cursor.close()

def main():
    logger.info("프로그램 시작")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(base_dir, os.pardir))
    local_env_path = os.path.join(parent_dir, '.env.local')

    # 단계 1: 환경 변수 로드
    logger.info("단계 1: 환경 변수 로드")
    env_config = load_env(local_env_path)

    # 단계 2: 데이터베이스 연결
    logger.info("단계 2: 데이터베이스 연결")
    conn = create_connection(env_config)

    # 단계 3: 데이터 조회 (티커, 계정 ID)
    logger.info("단계 3: 데이터 조회")
    tickers = get_company_tickers(conn)
    acc_names = ['매출액', '영업이익', '당기순이익', '자산총계', '부채총계', '자본총계']  # 예시 계정명
    acc_map = get_acc_ids(conn, acc_names)

    # 단계 4: 데이터 다운로드 및 재구성
    logger.info("단계 4: 데이터 다운로드 및 재구성")
    ticker_data = {}
    for i, (ticker, corp_code, corp_name) in enumerate(tickers, 1):
        for report_name, reprt_code in REPORT_CODES.items():
            year = get_latest_date(conn, ticker, reprt_code)
            logger.info("티커 %d/%d 다운로드 시작: %s, 연도 %s, 보고서 %s", i, len(tickers), ticker, year, report_name)
            data_df = download_and_reshape_data(ticker, corp_code, corp_name, year, reprt_code)
            if data_df is not None:
                ticker_data[(ticker, reprt_code)] = data_df
            logger.info("티커 %d/%d 다운로드 완료: %s, 보고서 %s", i, len(tickers), ticker, report_name)

    # 단계 5: 날짜 UUID 조회 및 ID 매핑
    logger.info("단계 5: 날짜 UUID 조회 및 ID 매핑")
    all_dates = set()
    for (ticker, _), data_df in ticker_data.items():
        dates = data_df['date'].apply(lambda x: x.replace(".", "-") if isinstance(x, str) else x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x)
        all_dates.update(dates)
    date_map = get_date_uuids(conn, list(all_dates))

    mapped_data = {}
    for i, ((ticker, reprt_code), data_df) in enumerate(ticker_data.items(), 1):
        logger.info("티커 %d/%d ID 매핑 시작: %s, 보고서 %s", i, len(ticker_data), ticker, reprt_code)
        mapped_df = map_data_to_ids(data_df, date_map, acc_map)
        if mapped_df is not None:
            mapped_data[(ticker, reprt_code)] = mapped_df
        logger.info("티커 %d/%d ID 매핑 완료: %s, 보고서 %s", i, len(ticker_data), ticker, reprt_code)

    # 단계 6: 데이터 삽입
    logger.info("단계 6: 데이터 삽입")
    start_time = time.time()
    for i, ((ticker, reprt_code), data_df) in enumerate(mapped_data.items(), 1):
        logger.info("티커 %d/%d 삽입 시작: %s, 보고서 %s", i, len(mapped_data), ticker, reprt_code)
        insert_financial_data(conn, ticker, data_df)
        logger.info("티커 %d/%d 삽입 완료: %s, 보고서 %s", i, len(mapped_data), ticker, reprt_code)

    end_time = time.time()
    logger.info("총 처리 시간: %.2f 초", end_time - start_time)
    logger.info("프로그램 종료")

    conn.close()

if __name__ == "__main__":
    main()