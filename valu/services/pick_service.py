# valu/services/pick_service.py
from valu.database import get_db_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_ticker_data(ticker: str) -> dict:
    logger.info(f"Fetching data for ticker: {ticker}")
    
    # account_raw_data 쿼리
    raw_query = """
        SELECT 
            ard.ticker,
            d.date,
            al.acc_name,
            ard.acc_raw_data
        FROM account_raw_data ard
        JOIN account_list al ON ard.acc_id = al.acc_id
        JOIN date d ON ard.date_id = d.date_id
        WHERE ard.ticker = %s 
          AND d.date BETWEEN CURRENT_DATE - INTERVAL '3 years' AND CURRENT_DATE
          AND al.acc_name IN ('open', 'high', 'low', 'close', 'volume', 'percent_change')
        ORDER BY d.date, al.acc_name
    """
    
    # account_processed_data 쿼리
    processed_query = """
        SELECT 
            apd.ticker,
            d.date,
            al.acc_name,
            apd.acc_processed_data
        FROM account_processed_data apd
        JOIN account_list al ON apd.acc_id = al.acc_id
        JOIN date d ON apd.date_id = d.date_id
        WHERE apd.ticker = %s 
          AND d.date BETWEEN CURRENT_DATE - INTERVAL '3 years' AND CURRENT_DATE
          AND al.acc_name IN ('Graham_Pick', 'Fisher_Pick', 'Lynch_Pick', 'Livermore_Pick', 'Minervini_Pick', 'ONeil_Pick')
        ORDER BY d.date, al.acc_name
    """
    
    conn = get_db_connection()
    try:
        ticker_data = {}
        
        # account_raw_data 조회
        with conn.cursor() as cur:
            cur.execute(raw_query, (ticker,))
            raw_results = cur.fetchall()
            logger.info(f"Fetched {len(raw_results)} raw data rows for ticker: {ticker}")
            
            for row in raw_results:
                date = str(row[1])
                acc_name = row[2]
                acc_raw_data = row[3]
                
                if date not in ticker_data:
                    ticker_data[date] = {"raw": {}, "processed": {}}
                if acc_name and acc_raw_data is not None:
                    ticker_data[date]["raw"][acc_name] = float(acc_raw_data)
                else:
                    logger.warning(f"NULL raw data for ticker {ticker}, date {date}, acc_name {acc_name}")
        
        # account_processed_data 조회
        with conn.cursor() as cur:
            cur.execute(processed_query, (ticker,))
            processed_results = cur.fetchall()
            logger.info(f"Fetched {len(processed_results)} processed data rows for ticker: {ticker}")
            
            for row in processed_results:
                date = str(row[1])
                acc_name = row[2]
                acc_processed_data = row[3]
                
                if date not in ticker_data:
                    ticker_data[date] = {"raw": {}, "processed": {}}
                if acc_name and acc_processed_data is not None:
                    ticker_data[date]["processed"][acc_name] = float(acc_processed_data)
                else:
                    logger.warning(f"NULL processed data for ticker {ticker}, date {date}, acc_name {acc_name}")
        
        return {
            "ticker": ticker,
            "data": ticker_data
        }
    except Exception as e:
        logger.error(f"Query failed for ticker {ticker}: {str(e)}")
        raise
    finally:
        conn.close()


def get_giant_picks(date: str, giant: str) -> dict:
    giant_name = f"{giant}_Pick"
    query = """
        SELECT 
            apd.ticker,
            ard.acc_name,
            ard.acc_raw_data
        FROM account_processed_data apd
        JOIN date d ON apd.date_id = d.date_id
        JOIN account_list al ON apd.acc_id = al.acc_id
        LEFT JOIN (
            SELECT ard.ticker, ard.acc_raw_data, al2.acc_name, ard.date_id
            FROM account_raw_data ard
            JOIN account_list al2 ON ard.acc_id = al2.acc_id
            WHERE al2.acc_name IN ('open', 'high', 'low', 'close', 'volume', 'percent_change')
        ) ard ON apd.ticker = ard.ticker AND apd.date_id = ard.date_id
        WHERE d.date = %s AND al.acc_name = %s AND apd.acc_processed_data = 1
        ORDER BY apd.ticker, ard.acc_name
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (date, giant_name))
            results = cur.fetchall()
            
            # 티커별로 데이터를 구조화
            tickers_data = {}
            for row in results:
                ticker = row[0]
                acc_name = row[1]
                acc_raw_data = row[2]
                
                if ticker not in tickers_data:
                    tickers_data[ticker] = {}
                if acc_name and acc_raw_data is not None:
                    tickers_data[ticker][acc_name] = float(acc_raw_data) if acc_raw_data else None
            
            # 티커 목록과 데이터 반환
            return {
                "date": date,
                "giant": giant_name,
                "tickers": tickers_data
            }
    finally:
        conn.close()