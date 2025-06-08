# create_company_table.py

import psycopg2
from dotenv import load_dotenv
import os

def create_connection():
    """
    .env에서 환경변수를 읽어서 psycopg2 커넥션 객체를 반환합니다.
    Supabase(혹은 Pooler) 연결 시에는 sslmode="require"를 꼭 붙여야 합니다.
    """
    load_dotenv()

    USER     = os.getenv("user")      # ex) postgres.프로젝트레퍼런스
    PASSWORD = os.getenv("password")  # Supabase에서 복사해 온 비밀번호
    HOST     = os.getenv("host")      # ex) aws-0-ap-northeast-2.pooler.supabase.com
    PORT     = int(os.getenv("port", 5432))
    DBNAME   = os.getenv("dbname")    # ex) postgres
    SSLMODE  = os.getenv("sslmode")

    conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        sslmode=SSLMODE
    )
    return conn

def create_company_table():
    """
    'company_list' 라는 이름의 테이블을 생성합니다.
    컬럼:
      - ticker       : TEXT PRIMARY KEY
      - cp_name      : TEXT NOT NULL
      - market       : TEXT NOT NULL
    """
    conn = None
    try:
        conn = create_connection()
        cursor = conn.cursor()

        create_sql = """
        CREATE TABLE IF NOT EXISTS company_list2 (
            ticker  TEXT    PRIMARY KEY,
            cp_name TEXT    NOT NULL,
            market  TEXT    NOT NULL
        );
        """

        cursor.execute(create_sql)
        conn.commit()
        print("✅ 테이블 'company_list' 생성(또는 이미 존재함).")

        cursor.close()
    except Exception as e:
        print("❌ 테이블 생성 중 에러 발생:", e)
    finally:
        if conn:
            conn.close()
            print("🔒 데이터베이스 연결 종료.")

if __name__ == "__main__":
    create_company_table()
