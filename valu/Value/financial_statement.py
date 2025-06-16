# 상장사의 재무제표를 가져오는 코드 (분기, 반기, 사업보고서 모두)
# 연결재무제표 우선
# 사업연도가 항상 '1월 1일~ 12월 31일'은 아님에 유의할 것
# 결과는 postgresql에 저장

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
from psycopg2.extensions import quote_ident
import re

# 🔑 API Key
API_KEY = api_key # 실제 API 키로 교체

# PostgreSQL 연결 정보
DB_INFO = {
    "user": "postgres",
    "password": "0307",
    "host": "localhost",
    "port": "5432",
    "database": "value"
}

# 보고서 코드
REPORT_CODES = {
    "q1": "11013",
    "semi": "11012",
    "q3": "11014",
    "annual": "11011"
}

FS_PRIORITY = ["CFS", "OFS"]

# 저장할 전체 필드 목록 (stock_code 제외, account_id 추가됨)
FULL_COLUMNS = [
    "ticker", "corp_name", "bsns_year", "account_id",
    "account_nm", "sj_div",
    "thstrm_amount", "thstrm_add_amount",
    "frmtrm_amount", "frmtrm_add_amount",
    "bfefrmtrm_amount", "currency"
]



def to_numeric_safe(val):
    try:
        return float(str(val).replace(",", "").strip()) if val else None
    except:
        return None

def read_ticker_csv(file_path='src_data/ticker_info.csv'):
    return pd.read_csv(file_path, dtype={'ticker': str, 'corp_code': str}).head(100)

def get_existing_tickers_from_db(table_name):
    try:
        conn = psycopg2.connect(**DB_INFO)
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT ticker FROM {quote_ident(table_name, cur)}")
        result = cur.fetchall()
        cur.close()
        conn.close()
        return set(row[0] for row in result)
    except Exception as e:
        print(f"[ERROR] 기존 ticker 조회 실패: {e}")
        return set()

def fetch_financial_data(corp_code, year, reprt_code):
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
                return data["list"]
            elif data.get("status") == "013":
                continue
        except Exception as e:
            print(f"[ERROR] API 요청 실패: {e}")
    return []

def clean_text(text):
    if not isinstance(text, str):
        return text
    return re.sub(r"\s+", "", text)

def parse_full_response(response, ticker, corp_name):
    parsed = []
    for item in response:
        if item.get("sj_div") == "SCE": # 자본변동표는 제외하고 저장
            continue 

        row = {
            "ticker": ticker,
            "corp_name": corp_name,
            "bsns_year": item.get("bsns_year"),
            "account_id": item.get("account_id"),
            "account_nm": clean_text(item.get("account_nm")),
            "sj_div": item.get("sj_div"),
            "thstrm_amount": to_numeric_safe(item.get("thstrm_amount")),
            "thstrm_add_amount": to_numeric_safe(item.get("thstrm_add_amount")),
            "frmtrm_amount": to_numeric_safe(item.get("frmtrm_amount")),
            "frmtrm_add_amount": to_numeric_safe(item.get("frmtrm_add_amount")),
            "bfefrmtrm_amount": to_numeric_safe(item.get("bfefrmtrm_amount")),
            "currency": item.get("currency")
        }
        parsed.append(row)
    return parsed

def save_to_postgresql(table_name, data_rows):
    if not data_rows:
        print(f"[INFO] 저장할 데이터 없음 → {table_name}")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_INFO)
        cur = conn.cursor()

        # 테이블 생성 (필드별 데이터 타입 구분)
        col_defs = ", ".join([
            f"{col} NUMERIC" if "amount" in col or col == "ord" else f"{col} TEXT"
            for col in FULL_COLUMNS
        ])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});")

        # 데이터 삽입
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(FULL_COLUMNS)}) VALUES %s",
            [tuple(row.get(col) for col in FULL_COLUMNS) for row in data_rows]
        )
        conn.commit()
        print(f"[OK] 저장 완료 → {table_name} ({len(data_rows)}건)")
        cur.close()
    except Exception as e:
        print(f"[ERROR] DB 저장 실패: {e}")
    finally:
        if conn:
            conn.close()


def main(year):
    ticker_df = read_ticker_csv()

    for report_name, reprt_code in REPORT_CODES.items():
        table_name = f"dart_{report_name}"
        existing_tickers = get_existing_tickers_from_db(table_name)
        
        for _, row in ticker_df.iterrows():
            ticker = row["ticker"]
            if ticker in existing_tickers:
                continue

            corp_code = row["corp_code"]
            corp_name = row.get("corp_name", "")
            print(f"📄 처리 중: {ticker} → {table_name}")
            response = fetch_financial_data(corp_code, year, reprt_code)
            parsed_rows = parse_full_response(response, ticker, corp_name)
            save_to_postgresql(table_name, parsed_rows)
            time.sleep(0.3)

    print(f"이미 저장되어 있는 ticker 목록: {existing_tickers}")

if __name__ == "__main__":
    main('2024')