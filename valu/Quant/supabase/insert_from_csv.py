# insert_from_csv.py

import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from io import StringIO

def create_connection():
    """
    .env에서 환경변수를 읽어 psycopg2 연결 객체를 반환합니다.
    """
    load_dotenv()

    USER     = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST     = os.getenv("host")
    PORT     = int(os.getenv("port", 5432))
    DBNAME   = os.getenv("dbname")

    conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        sslmode="require"
    )
    return conn

def insert_csv_to_company_info(csv_path: str):
    """
    company_data.csv 같은 CSV 파일을 읽어서, company_info 테이블에 한 번에 삽입합니다.
    - csv_path : 로컬에 있는 CSV 파일 경로
    """
    conn = None
    try:
        # 1) CSV 파일을 pandas로 읽어 옵니다.
        df = pd.read_csv(csv_path, dtype=str)  
        # 모든 칼럼을 문자열(str)로 읽으면, 숫자 섞여 있어도 TEXT로 삽입하기 편합니다.

        # 2) DataFrame을 메모리상의 StringIO 객체로 변환 (CSV 포맷으로)
        buffer = StringIO()
        # header=True → CSV의 첫 줄에 컬럼명(ticker,company_name,market_name)을 포함
        df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)  # 버퍼를 다시 처음 위치로 돌립니다.

        # 3) DB 연결 및 COPY 실행
        conn = create_connection()
        cursor = conn.cursor()

        # company_info 테이블의 컬럼 순서(ticker, company_name, market_name)에 맞추어 복사
        copy_sql = """
            COPY company_list (ticker, cp_name, market)
            FROM STDIN WITH CSV HEADER
        """
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f"✅ '{csv_path}' 파일의 데이터를 company_info 테이블에 성공적으로 삽입했습니다.")
        cursor.close()

    except Exception as e:
        print("❌ 데이터 삽입 중 오류 발생:", e)
    finally:
        if conn:
            conn.close()
            print("🔒 데이터베이스 연결 종료.")

if __name__ == "__main__":
    # 실제 CSV 파일 경로를 지정하세요.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(base_dir, 'ticker_names.csv')
    insert_csv_to_company_info(csv_file_path)
