import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from pathlib import Path
import re
from dotenv import load_dotenv

# Load environment variables from .env file in supabase folder
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Configure logging
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), 'migration_errors.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database connection parameters from .env
DB_CONFIG = {
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'database': os.getenv('dbname'),
    'user': os.getenv('user'),
    'password': os.getenv('password')
}

# Data directories and corresponding table names
DATA_DIRS = {
    'balance_sheet': 'balance_sheet',
    'cash_flow': 'cash_flow',
    'financials': 'financials',
    'indicators': 'indicators',
    'giants_pick': 'giants_picks',
    'stock_data': 'stock_data',
    'quarterly_balance_sheet': 'balance_sheet',
    'quarterly_cash_flow': 'cash_flow',
    'quarterly_financials': 'financials'
}

# Batch size for inserting data to manage memory
BATCH_SIZE = 10000

def create_db_engine():
    """Create SQLAlchemy engine for PostgreSQL."""
    try:
        connection_string = (
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        logging.error(f"Failed to create DB engine: {str(e)}")
        raise

def ensure_table_exists(engine, table_name):
    """Ensure the target table exists, create if not."""
    table_definitions = {
        'stock_data': """
            CREATE TABLE IF NOT EXISTS stock_data (
                ticker VARCHAR(20),
                date DATE,
                open DECIMAL,
                high DECIMAL,
                low DECIMAL,
                close DECIMAL,
                volume BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """,
        'balance_sheet': """
            CREATE TABLE IF NOT EXISTS balance_sheet (
                ticker VARCHAR(20),
                date DATE,
                total_assets DECIMAL,
                total_liabilities DECIMAL,
                equity DECIMAL,
                is_quarterly BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """,
        'cash_flow': """
            CREATE TABLE IF NOT EXISTS cash_flow (
                ticker VARCHAR(20),
                date DATE,
                operating_cash_flow DECIMAL,
                investing_cash_flow DECIMAL,
                financing_cash_flow DECIMAL,
                is_quarterly BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """,
        'financials': """
            CREATE TABLE IF NOT EXISTS financials (
                ticker VARCHAR(20),
                date DATE,
                revenue DECIMAL,
                net_income DECIMAL,
                eps DECIMAL,
                is_quarterly BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """,
        'indicators': """
            CREATE TABLE IF NOT EXISTS indicators (
                ticker VARCHAR(20),
                date DATE,
                rsi DECIMAL,
                macd DECIMAL,
                relative_strength DECIMAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """,
        'giants_picks': """
            CREATE TABLE IF NOT EXISTS giants_picks (
                ticker VARCHAR(20),
                date DATE,
                is_giant BOOLEAN,
                score DECIMAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """,
        'tickers': """
            CREATE TABLE IF NOT EXISTS tickers (
                ticker VARCHAR(20) PRIMARY KEY,
                company_name VARCHAR(100),
                market VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    }
    try:
        with engine.connect() as conn:
            conn.execute(text(table_definitions[table_name]))
            conn.commit()
    except Exception as e:
        logging.error(f"Failed to create table {table_name}: {str(e)}")
        raise

def migrate_feather_file(file_path, table_name, engine, is_quarterly=False):
    """Migrate a single feather file to PostgreSQL."""
    try:
        # Extract ticker from filename
        ticker = re.search(r'(\d+\.KS)', file_path).group(1)
        logging.info(f"Processing {file_path} for ticker {ticker}")

        # Read feather file
        df = pd.read_feather(file_path)
        
        # Add ticker and is_quarterly columns
        df['ticker'] = ticker
        if 'is_quarterly' in df.columns or table_name in ['balance_sheet', 'cash_flow', 'financials']:
            df['is_quarterly'] = is_quarterly
        df['created_at'] = pd.Timestamp.now()

        # Ensure date is in correct format
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date

        # Insert data in batches
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i + BATCH_SIZE]
            batch.to_sql(table_name, engine, if_exists='append', index=False)
            logging.info(f"Inserted {len(batch)} rows into {table_name} for ticker {ticker}")

    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")

def migrate_tickers_file(file_path, engine):
    """Migrate tickers.csv to tickers table."""
    try:
        df = pd.read_csv(file_path)
        df['created_at'] = pd.Timestamp.now()
        df.to_sql('tickers', engine, if_exists='append', index=False)
        logging.info(f"Inserted {len(df)} rows into tickers table")
    except Exception as e:
        logging.error(f"Error processing tickers.csv: {str(e)}")

def main():
    engine = create_db_engine()

    # Create all necessary tables
    for table_name in set(DATA_DIRS.values()).union({'tickers'}):
        ensure_table_exists(engine, table_name)

    # Base directory for feather files (relative to supabase/data_migration)
    base_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'src_data')

    # Migrate tickers.csv
    tickers_file = os.path.join(base_dir, 'ticker_list', 'tickers.csv')
    if os.path.exists(tickers_file):
        migrate_tickers_file(tickers_file, engine)

    # Migrate feather files
    for folder, table_name in DATA_DIRS.items():
        folder_path = os.path.join(base_dir, folder)
        if not os.path.exists(folder_path):
            logging.warning(f"Folder {folder_path} does not exist")
            continue

        is_quarterly = 'quarterly' in folder
        feather_files = glob.glob(os.path.join(folder_path, '*.feather'))
        for file_path in feather_files:
            migrate_feather_file(file_path, table_name, engine, is_quarterly)

    logging.info("Migration completed")

if __name__ == "__main__":
    main()