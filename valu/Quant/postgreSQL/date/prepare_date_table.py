import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import holidays
from psycopg2 import IntegrityError

def load_env(env_file):
    load_dotenv(env_file, override=True)
    return {
        "user": os.getenv("user"),
        "password": os.getenv("password"),
        "host": os.getenv("host"),
        "port": int(os.getenv("port", 5432)),
        "dbname": os.getenv("dbname"),
        "sslmode": os.getenv("sslmode", "disable")
    }

def create_connection(env_config):
    conn_params = {
        "user": env_config["user"],
        "password": env_config["password"],
        "host": env_config["host"],
        "port": env_config["port"],
        "dbname": env_config["dbname"]
    }
    if env_config["sslmode"]:
        conn_params["sslmode"] = env_config["sslmode"]
    return psycopg2.connect(**conn_params)

def prepare_date_table(conn, start_date, end_date):
    cursor = conn.cursor()
    kr_holidays = holidays.KR()
    current = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        is_holiday = date_str in kr_holidays

        try:
            cursor.execute(
                """
                INSERT INTO date
                    (date_id, year, quarter, month, week, day, date, is_holiday)
                VALUES
                    (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_id) DO NOTHING;
                """,
                (
                    current.year,
                    (current.month - 1) // 3 + 1,
                    current.month,
                    current.isocalendar()[1],
                    current.day,
                    date_str,
                    is_holiday
                )
            )
            # 한 행씩 커밋
            conn.commit()

        except IntegrityError as ie:
            # 제약 위반 시 현재 행만 롤백하고 넘어감
            conn.rollback()
            print(f"⚠️  제약 위반으로 건너뜀: {date_str} → {ie}")

        except Exception as e:
            # 기타 에러도 현재 행만 롤백
            conn.rollback()
            print(f"❌ 삽입 중 오류 발생 ({date_str}): {e}")

        # 다음 날짜로 이동
        current += timedelta(days=1)

    cursor.close()
    print("✅ Date table 준비 완료.")

def main():
    base_dir   = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(base_dir, os.pardir))
    env_path   = os.path.join(parent_dir, ".env.postgres")

    env_config = load_env(env_path)
    conn       = create_connection(env_config)

    end_date   = (datetime.now() + timedelta(days=365)).strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y%m%d")

    prepare_date_table(conn, start_date, end_date)
    conn.close()

if __name__ == "__main__":
    main()
