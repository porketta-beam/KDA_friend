from valu.database import get_db_connection

def get_giant_picks(date: str, giant: str) -> list[str]:
    giant_name = f"{giant}_Pick"
    query = """
        SELECT apd.ticker
        FROM account_processed_data apd
        JOIN date d ON apd.date_id = d.date_id
        JOIN account_list al ON apd.acc_id = al.acc_id
        WHERE d.date = %s AND al.acc_name = %s AND apd.acc_processed_data = 1
    """
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(query, (date, giant_name))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
