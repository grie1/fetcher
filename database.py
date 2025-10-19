import sqlite3
import pandas as pd
from datetime import datetime
import logging

DB_PATH = 'gme_data.db'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def init_db(drop_table=False):  # Make DROP optional (default False for prod)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if drop_table:
        logger.warning("Dropping table for fresh init (use sparingly).")
        cursor.execute('DROP TABLE IF EXISTS options_data')
    else:
        logger.info("Initializing DB (no drop).")
    
    # Schema (unchanged)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS options_data (
        date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        contract_symbol TEXT,
        put_call TEXT,
        strike_price REAL,
        expiration_date TEXT,
        open_interest INTEGER,
        volume INTEGER,
        last_price REAL,
        bid REAL,
        ask REAL,
        source TEXT NOT NULL,
        ingest_timestamp TEXT NOT NULL,
        UNIQUE(date, ticker, contract_symbol)
    )
    ''')
    
    # Index (unchanged)
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_date_ticker ON options_data (date, ticker)')
    
    conn.commit()
    conn.close()
    logger.info("DB initialized.")

def get_last_date(table: str, conn) -> str:
    df = pd.read_sql(f"SELECT MAX(date) FROM {table}", conn)
    last = df.iloc[0, 0] if not df.empty and pd.notna(df.iloc[0, 0]) else '1900-01-01'
    logger.debug(f"Last date in {table}: {last}")
    return last

def insert_data(df: pd.DataFrame, table: str, conn):
    if df.empty:
        logger.warning(f"Empty DataFrame for {table}, skipping insert.")
        return
    
    try:
        pre_count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
        df.to_sql(table, conn, if_exists='append', index=False, method='multi')  # 'multi' for batch efficiency
        post_count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
        inserted = post_count - pre_count
        logger.info(f"{table}: Attempted {len(df)} rows; {inserted} actually inserted (upserts handled by UNIQUE).")
        conn.commit()
    except Exception as e:
        logger.error(f"Insert failed for {table}: {e}")
        conn.rollback()
