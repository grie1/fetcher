import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = 'gme_data.db'

def init_db():
    """Initialize SQLite DB with options_data table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Schema for options_data (as discussed)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS options_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        UNIQUE(date, ticker, contract_symbol)  -- Prevent duplicates
    )
    ''')
    
    # Index for fast queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_date_ticker ON options_data (date, ticker)')
    
    conn.commit()
    conn.close()
    print("DB initialized with options_data table.")

def insert_data(df: pd.DataFrame, table: str = 'options_data'):
    """Insert or ignore DataFrame rows (delta-safe via UNIQUE constraint)."""
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(table, conn, if_exists='append', index=False, method='multi')
    conn.close()
    print(f"Inserted {len(df)} rows into {table}.")

def check_exists(date: str, ticker: str) -> bool:
    """Check if data for date/ticker already exists."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM options_data WHERE date = ? AND ticker = ?", (date, ticker))
    exists = cursor.fetchone()[0] > 0
    conn.close()
    return exists

def query_sample(limit: int = 5) -> pd.DataFrame:
    """Sample query: Last N rows for GME today."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT * FROM options_data 
        WHERE ticker = 'GME' AND date = '{datetime.now().strftime('%Y-%m-%d')}'
        ORDER BY id DESC LIMIT {limit}
    """, conn)
    conn.close()
    return df
