import psycopg2
from valu.config import DB_CONFIG

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)
