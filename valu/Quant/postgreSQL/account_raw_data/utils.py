import psycopg2
from dotenv import load_dotenv
import os
import logging
import pandas as pd

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
    cursor.execute("SELECT ticker, corp_code, corp_name FROM company_list;")
    tickers = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
    cursor.close()
    logger.info("티커 목록 조회 완료: %d개 티커", len(tickers))
    return tickers

def get_date_uuids(conn, dates):
    logger.info("날짜 UUID 매핑 조회 시작: %d개 날짜", len(dates))
    cursor = conn.cursor()
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
    if len(date_map) < len(dates):
        logger.warning("일부 날짜에 대한 date_id가 누락됨: %s", set(date_list) - set(date_map.keys()))
    return date_map

def get_acc_ids(conn, acc_names):
    logger.info("계정 ID 매핑 조회 시작")
    cursor = conn.cursor()
    query = """
    SELECT acc_name, acc_id
    FROM account_list
    WHERE acc_name IN %s;
    """
    cursor.execute(query, (tuple(acc_names),))
    acc_map = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    logger.info("계정 ID 매핑 조회 완료: %d개 계정", len(acc_map))
    for acc_name in acc_names:
        if acc_name not in acc_map:
            logger.error("account_list에 '%s' 계정명이 누락됨", acc_name)
    return acc_map

def to_numeric_safe(val):
    try:
        return float(str(val).replace(",", "").strip()) if val else None
    except:
        return None