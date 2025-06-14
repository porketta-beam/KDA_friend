import psycopg2
from dotenv import load_dotenv
import os
from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd
import logging
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env(env_file):
    logger.info("환경 변수 파일 로드 시작: %s", env_file)
    load_dotenv(env_file, override=True)
    env_config = {
        "user": os.getenv("user"),
        "password": os.getenv("password"),
        "host": os.getenv("host"),
        "port": int(os.getenv("port", 5432)),
        "dbname": os.getenv("dbname"),
        "sslmode": os.getenv("sslmode", "disable")
    }
    logger.info("환경 변수 로드 완료")
    return env_config

def create_connection(env_config):
    logger.info("데이터베이스 연결 시작: %s:%s", env_config["host"], env_config["port"])
    conn_params = {
        "user": env_config["user"],
        "password": env_config["password"],
        "host": env_config["host"],
        "port": env_config["port"],
        "dbname": env_config["dbname"]
    }
    if env_config["sslmode"]:
        conn_params["sslmode"] = env_config["sslmode"]
    conn = psycopg2.connect(**conn_params)
    logger.info("데이터베이스 연결 성공")
    return conn

def get_company_tickers(conn):
    logger.info("티커 목록 조회 시작")
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM company_list;")
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    logger.info("티커 목록 조회 완료: %d개 티커", len(tickers))
    # tickers = tickers[5:15]
    return tickers

def get_date_uuids(conn, dates):
    logger.info("날짜 UUID 매핑 조회 시작: %d개 날짜", len(dates))
    cursor = conn.cursor()
    # 필요한 날짜만 조회하도록 수정
    date_list = [date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else date for date in dates]
    query = """
    SELECT date, date_id
    FROM date
    WHERE date::text IN %s;
    """
    cursor.execute(query, (tuple(date_list),))
    date_map = {row[0].strftime('%Y-%m-%d'): row[1] for row in cursor.fetchall()}
    cursor.close()
    logger.info("날짜 UUID 매핑 조회 완료: %d/%d개 날짜 매핑", len(date_map), len(dates))
    return date_map

def get_acc_ids(conn):
    logger.info("계정 ID 매핑 조회 시작")
    cursor = conn.cursor()
    # 필요한 계정명만 조회하도록 수정
    query = """
    SELECT acc_name, acc_id
    FROM account_list
    WHERE acc_name IN ('open', 'high', 'low', 'close', 'volume', 'percent_change');
    """
    cursor.execute(query)
    acc_map = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    logger.info("계정 ID 매핑 조회 완료: %d개 계정", len(acc_map))
    return acc_map

def download_and_reshape_data(ticker, start_date, end_date):
    logger.info("티커 %s 데이터 다운로드 시작: %s ~ %s", ticker, start_date, end_date)
    try:
        df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
        if df.empty:
            logger.warning("티커 %s에 대한 데이터 없음", ticker)
            return None
        logger.info("티커 %s 데이터 다운로드 완료: %d 행", ticker, len(df))
        # 인덱스를 'date' 컬럼으로 변환
        df = df.reset_index().rename(columns={'날짜': 'date'})
        # OHLCV를 melt로 재구성
        melted_df = pd.melt(df, id_vars=['date'], 
                           value_vars=['시가', '고가', '저가', '종가', '거래량', '등락률'],
                           var_name='data_type', value_name='value')
        melted_df['ticker'] = ticker
        melted_df['data_type'] = melted_df['data_type'].map({
            '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', '거래량': 'volume', '등락률': 'percent_change'
        })
        logger.info("티커 %s 데이터 재구성 완료: %d 행", ticker, len(melted_df))
        return melted_df[['date', 'ticker', 'data_type', 'value']]
    except Exception as e:
        logger.error("티커 %s 데이터 다운로드/재구성 오류: %s", ticker, e)
        return None

def map_data_to_ids(data_df, date_map, acc_map):
    logger.info("데이터프레임 ID 매핑 시작")
    try:
        # date 열을 date_id로 변환
        data_df['date_id'] = data_df['date'].map(lambda x: date_map.get(x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x))
        # data_type 열을 acc_id로 변환
        data_df['acc_id'] = data_df['data_type'].map(lambda x: acc_map.get(x))
        # 유효하지 않은 date_id 또는 acc_id는 제외
        original_len = len(data_df)
        data_df = data_df.dropna(subset=['date_id', 'acc_id'])
        logger.info("데이터프레임 ID 매핑 완료: %d/%d 행 유지", len(data_df), original_len)
        return data_df[['date_id', 'ticker', 'acc_id', 'value']]
    except Exception as e:
        logger.error("데이터프레임 ID 매핑 오류: %s", e)
        return None

def insert_stock_data(conn, ticker, data_df):
    logger.info("티커 %s 데이터 삽입 시작", ticker)
    cursor = conn.cursor()
    try:
        batch_data = []
        for _, row in data_df.iterrows():
            value = float(row['value']) if pd.notna(row['value']) else None
            batch_data.append((row['date_id'], ticker, row['acc_id'], value))

        if batch_data:
            cursor.executemany(
                """
                INSERT INTO account_raw_data (date_id, ticker, acc_id, acc_raw_data)
                VALUES (%s, %s, %s, %s)
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
    local_env_path = os.path.join(parent_dir, '.env.postgres')

    # 단계 1: 환경 변수 로드
    logger.info("단계 1: 환경 변수 로드")
    env_config = load_env(local_env_path)

    # 단계 2: 데이터베이스 연결
    logger.info("단계 2: 데이터베이스 연결")
    conn = create_connection(env_config)
    
    # 단계 3: 데이터 조회 (티커, 계정 ID)
    logger.info("단계 3: 데이터 조회")
    tickers = get_company_tickers(conn)
    acc_map = get_acc_ids(conn)

    # 단계 4: 데이터 다운로드 및 재구성
    logger.info("단계 4: 데이터 다운로드 및 재구성")
    ticker_data = {}
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=370)).strftime('%Y%m%d')
    logger.info("데이터 수집 기간: %s ~ %s", start_date, end_date)

    for i, ticker in enumerate(tickers, 1):
        logger.info("티커 %d/%d 다운로드 시작: %s", i, len(tickers), ticker)
        data_df = download_and_reshape_data(ticker, start_date, end_date)
        if data_df is not None:
            ticker_data[ticker] = data_df
        logger.info("티커 %d/%d 다운로드 완료: %s", i, len(tickers), ticker)

    # 단계 5: 날짜 UUID 조회 및 ID 매핑
    logger.info("단계 5: 날짜 UUID 조회 및 ID 매핑")
    # 모든 티커의 날짜를 수집하여 중복 제거
    all_dates = set()
    for ticker, data_df in ticker_data.items():
        dates = data_df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x)
        all_dates.update(dates)
    date_map = get_date_uuids(conn, list(all_dates))

    mapped_data = {}
    for i, (ticker, data_df) in enumerate(ticker_data.items(), 1):
        logger.info("티커 %d/%d ID 매핑 시작: %s", i, len(tickers), ticker)
        mapped_df = map_data_to_ids(data_df, date_map, acc_map)
        if mapped_df is not None:
            mapped_data[ticker] = mapped_df
        logger.info("티커 %d/%d ID 매핑 완료: %s", i, len(tickers), ticker)

    # 단계 6: 데이터 삽입
    logger.info("단계 6: 데이터 삽입")
    start_time = time.time()
    for i, (ticker, data_df) in enumerate(mapped_data.items(), 1):
        logger.info("티커 %d/%d 삽입 시작: %s", i, len(tickers), ticker)
        insert_stock_data(conn, ticker, data_df)
        logger.info("티커 %d/%d 삽입 완료: %s", i, len(tickers), ticker)

    end_time = time.time()
    logger.info("총 처리 시간: %.2f 초", end_time - start_time)
    logger.info("프로그램 종료")

    conn.close()

if __name__ == "__main__":
    main()