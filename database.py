import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = 'gme_data.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Drop and recreate for schema update (safe for dev; comment out for prod)
    cursor.execute('DROP TABLE IF EXISTS options_data')
    
    # New schema matching current DF
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
    
    # Index for fast queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_date_ticker ON options_data (date, ticker)')
    
    conn.commit()
    conn.close()
    print("DB initialized.")

def get_last_date(table: str, conn) -> str:
    df = pd.read_sql(f"SELECT MAX(date) FROM {table}", conn)
    return df.iloc[0, 0] if not df.empty and pd.notna(df.iloc[0, 0]) else '1900-01-01'

def insert_data(df: pd.DataFrame, table: str, conn):
    df.to_sql(table, conn, if_exists='append', index=False)
