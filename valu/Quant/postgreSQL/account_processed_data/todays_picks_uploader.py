import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('todays_pick_uploader.log')
    ]
)
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
        "password": os.getenv("password"),
        "host": env_config["host"],
        "port": env_config["port"],
        "dbname": env_config["dbname"]
    }
    if env_config["sslmode"]:
        conn_params["sslmode"] = env_config["sslmode"]
    conn = psycopg2.connect(**conn_params)
    logger.info("데이터베이스 연결 성공")
    return conn

def get_date_ids(conn):
    logger.info("날짜 ID 매핑 조회 시작")
    cursor = conn.cursor()
    cursor.execute("SELECT date, date_id FROM date")
    date_map = {row[0].strftime('%Y-%m-%d'): row[1] for row in cursor.fetchall()}
    cursor.close()
    logger.info("날짜 ID 매핑 조회 완료: %d개", len(date_map))
    return date_map

def get_acc_ids(conn):
    logger.info("계정 ID 매핑 조회 시작")
    cursor = conn.cursor()
    cursor.execute("SELECT acc_name, acc_id FROM account_list")
    acc_map = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    logger.info("계정 ID 매핑 조회 완료: %d개", len(acc_map))
    return acc_map

def upload_todays_picks(conn, picks_file):
    logger.info("Today's Picks 업로드 시작: %s", picks_file)
    try:
        # feather 파일 읽기
        df = pd.read_feather(picks_file)
        logger.info("Feather 파일 읽기 완료: %d 행", len(df))

        # Giant를 계정명으로 매핑
        giant_to_acc = {
            'Benjamin Graham': 'Graham_Pick',
            'Ken Fisher': 'Fisher_Pick',
            'Peter Lynch': 'Lynch_Pick',
            'Jesse Livermore': 'Livermore_Pick',
            'Mark Minervini': 'Minervini_Pick',
            'William Oneil': 'ONeil_Pick'
        }
        df['acc_name'] = df['Giant'].map(giant_to_acc)

        # 날짜 및 계정 ID 매핑 미리 가져오기
        date_map = get_date_ids(conn)
        acc_map = get_acc_ids(conn)

        # 데이터 처리 및 개별 삽입
        successful_inserts = 0
        for _, row in df.iterrows():
            # 새로운 커서 생성 (독립적인 트랜잭션)
            cursor = conn.cursor()
            try:
                # 날짜 ID 확인
                date_id = date_map.get(row['Date'])
                if date_id is None:
                    logger.warning("해당 날짜에 대한 ID가 없습니다: %s, 행 건너뜀", row['Date'])
                    cursor.close()
                    continue

                # 계정 ID 확인
                acc_id = acc_map.get(row['acc_name'])
                if acc_id is None:
                    logger.warning("해당 계정명에 대한 ID가 없습니다: %s, 행 건너뜀", row['acc_name'])
                    cursor.close()
                    continue

                # 티커 형식 변환 ('000000.XX' -> '000000')
                try:
                    ticker = row['Ticker'].split('.')[0]
                except (AttributeError, IndexError):
                    logger.warning("잘못된 티커 형식: %s, 행 건너뜀", row['Ticker'])
                    cursor.close()
                    continue

                # 개별 행 삽입
                insert_data = (
                    str(uuid.uuid4()),  # acc_processed_id (UUID)
                    date_id,
                    ticker,
                    acc_id,
                    1.0000  # acc_raw_data (boolean을 numeric으로 표현)
                )
                cursor.execute("""
                    INSERT INTO account_processed_data (acc_processed_id, date_id, ticker, acc_id, acc_processed_data)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, insert_data)
                conn.commit()
                successful_inserts += 1
                logger.info("행 삽입 성공: 티커 %s, 날짜 %s", ticker, row['Date'])
            except psycopg2.Error as e:
                conn.rollback()
                logger.warning("제약 조건 위반 또는 오류 발생: %s, 티커: %s, 행 건너뜀", e, ticker)
            finally:
                cursor.close()

        logger.info("데이터 업로드 완료: %d 행 성공", successful_inserts)
    except Exception as e:
        conn.rollback()
        logger.error("데이터 업로드 중 전체 오류: %s", e)
    finally:
        if 'cursor' in locals():
            cursor.close()

def main():
    logger.info("프로그램 시작")
    base_dir = Path(__file__).parent.parent.parent / "src_data"
    picks_file = base_dir / "todays_picks" / "todays_picks.feather"
    local_env_path = Path(__file__).parent.parent / ".env.postgres"

    # 환경 변수 로드
    env_config = load_env(local_env_path)

    # 데이터베이스 연결
    conn = create_connection(env_config)

    # Today's Picks 업로드
    upload_todays_picks(conn, picks_file)

    conn.close()
    logger.info("프로그램 종료")

if __name__ == "__main__":
    import uuid
    main()