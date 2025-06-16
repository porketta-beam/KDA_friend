from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
print("Looking for .env at:", dotenv_path)  # 디버깅 출력
load_dotenv(dotenv_path, override=True)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "dbname": os.getenv("DB_NAME")
}
print("DB_CONFIG:", DB_CONFIG)  # 디버깅 출력 추가