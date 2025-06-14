# insert_from_csv.py

import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from io import StringIO
from typing import List, Optional


def create_connection(env_path) -> psycopg2.extensions.connection:
    """
    .env에서 환경변수를 읽어 psycopg2 연결 객체를 반환합니다.
    """
    load_dotenv(env_path, override=True)

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
        # sslmode="require"
    )
    return conn


def insert_csv_to_table(
    env_path: str,    
    csv_path: str,
    table_name: str,
    columns: Optional[List[str]] = None
) -> None:
    """
    CSV 파일을 읽어 지정된 테이블에 한 번에 삽입합니다.

    - csv_path   : 로컬에 있는 CSV 파일 경로
    - table_name : 삽입할 대상 테이블명
    - columns    : CSV에서 사용할 컬럼명 리스트 (None일 경우 CSV 헤더 전체 사용)
    """
    conn = None
    try:
        # 1) CSV 파일을 pandas로 읽어 옵니다.
        df = pd.read_csv(csv_path, dtype=str)  
        # 모든 칼럼을 문자열(str)로 읽으면, 숫자 섞여 있어도 TEXT로 삽입하기 편합니다.

        # 2) DataFrame을 메모리상의 StringIO 객체로 변환 (CSV 포맷으로)
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)

        # 3) DB 연결 및 COPY 실행
        conn = create_connection(env_path)
        cursor = conn.cursor()

        # 컬럼 리스트 SQL 생성
        if columns:
            col_list_sql = f"({', '.join(columns)})"
        else:
            col_list_sql = f"({', '.join(df.columns.tolist())})"

        copy_sql = f"""
            COPY {table_name} {col_list_sql}
            FROM STDIN WITH CSV HEADER
        """
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f"✅ '{csv_path}' 데이터를 '{table_name}' 테이블에 성공적으로 삽입했습니다.")
        cursor.close()

    except Exception as e:
        print("❌ 데이터 삽입 중 오류 발생:", e)
    finally:
        if conn:
            conn.close()
            print("🔒 데이터베이스 연결 종료.")


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(base_dir, os.pardir))
    env_path = os.path.join(parent_dir, '.env.local')
    
    # 예시: company_info 테이블에 ticker_names.csv 삽입
    csv_file = os.path.join(base_dir, 'account_list_filled.csv')
    table = 'account_list'
    # cols = ['ticker', 'company_name', 'market_name']  # CSV 헤더에 맞춰 지정

    insert_csv_to_table(env_path, csv_file, table)
