import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = 'gme_data.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS options_data (
        date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        OCCSymbol TEXT,
        Underlying TEXT,
        PutCall TEXT,
        Strike REAL,
        Expiry TEXT,
        OI INTEGER,
        Volume INTEGER,
        source TEXT NOT NULL,
        ingest_timestamp TEXT NOT NULL,
        UNIQUE(date, ticker, OCCSymbol)
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_date_ticker ON options_data (date, ticker)')
    
    conn.commit()
    conn.close()
    print("DB initialized.")

def get_last_date(table: str, conn) -> str:
    df = pd.read_sql(f"SELECT MAX(date) FROM {table}", conn)
    return df.iloc[0, 0] if not df.empty and pd.notna(df.iloc[0, 0]) else '1900-01-01'

def insert_data(df: pd.DataFrame, table: str, conn):
    df.to_sql(table, conn, if_exists='append', index=False)
