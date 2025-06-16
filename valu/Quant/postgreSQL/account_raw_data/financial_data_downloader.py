import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
from psycopg2.extensions import quote_ident

# 🔑 API Key
API_KEY = '58d6b02297b245067c9e7d9f1a32639ca1140072'  # 실제 API 키로 교체

# PostgreSQL 연결 정보
DB_INFO = {
    "user": "postgres",
    "password": "0000",
    "host": "localhost",
    "port": "5432",
    "database": "valu_test1"
}

# 보고서 코드
REPORT_CODES = {
    "q1": "11013",
    "semi": "11012",
    "q3": "11014",
    "annual": "11011"
}

FS_PRIORITY = ["CFS", "OFS"]

# 저장할 전체 필드 목록
FULL_COLUMNS = [
    "ticker", "corp_name", "rcept_no", "bsns_year", "reprt_code", "account_id",
    "account_nm", "fs_div", "fs_nm", "sj_div", "sj_nm",
    "thstrm_nm", "thstrm_dt", "thstrm_amount", "thstrm_add_amount",
    "frmtrm_nm", "frmtrm_dt", "frmtrm_amount", "frmtrm_add_amount",
    "bfefrmtrm_nm", "bfefrmtrm_dt", "bfefrmtrm_amount",
    "ord", "currency"
]

def to_numeric_safe(val):
    try:
        return float(str(val).replace(",", "").strip()) if val else None
    except:
        return None

def read_ticker_csv(file_path='ticker_info.csv'):
    return pd.read_csv(file_path, dtype={'ticker': str, 'corp_code': str}).head(2)

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

def parse_full_response(response, ticker, corp_name):
    parsed = []
    for item in response:
        row = {
            "ticker": ticker,
            "corp_name": corp_name,
            "rcept_no": item.get("rcept_no"),
            "bsns_year": item.get("bsns_year"),
            "reprt_code": item.get("reprt_code"),
            "account_id": item.get("account_id"),
            "account_nm": item.get("account_nm"),
            "fs_div": item.get("fs_div"),
            "fs_nm": item.get("fs_nm"),
            "sj_div": item.get("sj_div"),
            "sj_nm": item.get("sj_nm"),
            "thstrm_nm": item.get("thstrm_nm"),
            "thstrm_dt": item.get("thstrm_dt"),
            "thstrm_amount": to_numeric_safe(item.get("thstrm_amount")),
            "thstrm_add_amount": to_numeric_safe(item.get("thstrm_add_amount")),
            "frmtrm_nm": item.get("frmtrm_nm"),
            "frmtrm_dt": item.get("frmtrm_dt"),
            "frmtrm_amount": to_numeric_safe(item.get("frmtrm_amount")),
            "frmtrm_add_amount": to_numeric_safe(item.get("frmtrm_add_amount")),
            "bfefrmtrm_nm": item.get("bfefrmtrm_nm"),
            "bfefrmtrm_dt": item.get("bfefrmtrm_dt"),
            "bfefrmtrm_amount": to_numeric_safe(item.get("bfefrmtrm_amount")),
            "ord": to_numeric_safe(item.get("ord")),
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

        col_defs = ", ".join([
            f"{col} NUMERIC" if "amount" in col or col == "ord" else f"{col} TEXT"
            for col in FULL_COLUMNS
        ])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});")

        clean_rows = []
        for row in data_rows:
            clean_row = tuple(
                to_numeric_safe(row.get(col)) if "amount" in col or col == "ord"
                else (row.get(col) if row.get(col) != "" else None)
                for col in FULL_COLUMNS
            )
            clean_rows.append(clean_row)

        execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(FULL_COLUMNS)}) VALUES %s",
            clean_rows
        )
        conn.commit()
        print(f"[OK] 저장 완료 → {table_name} ({len(clean_rows)}건)")
        cur.close()
    except Exception as e:
        print(f"[ERROR] DB 저장 실패: {e}")
    finally:
        if conn:
            conn.close()

def main(year):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ticker_path = os.path.join(base_dir, 'ticker_info.csv')
    ticker_df = read_ticker_csv(ticker_path)

    for report_name, reprt_code in REPORT_CODES.items():
        table_name = f"dart_{report_name}_1"
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

            if not parsed_rows:
                print(f"[INFO] 파싱된 데이터 없음: {ticker} ({report_name})")
                continue

            save_to_postgresql(table_name, parsed_rows)
            time.sleep(0.3)

    print(f"✔️ 전체 처리 완료.")

if __name__ == "__main__":
    main('2024')
