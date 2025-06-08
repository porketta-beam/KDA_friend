import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

print("▶ USER   :", USER)
print("▶ PASSWORD:", PASSWORD)
print("▶ HOST   :", HOST)
print("▶ PORT   :", PORT)
print("▶ DBNAME :", DBNAME)

try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        sslmode="require"
    )
    print("Connection successful!")
    cursor = connection.cursor()
    cursor.execute("SELECT NOW();")
    print("Current Time:", cursor.fetchone())
    cursor.close()
    connection.close()
    print("Connection closed.")
except psycopg2.OperationalError as oe:
    print("OperationalError args:", oe.args)
    print("OperationalError pgerror:", oe.pgerror)
except Exception as e:
    print("Failed to connect:", repr(e))
