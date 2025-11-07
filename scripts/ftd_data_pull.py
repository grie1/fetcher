#!/usr/bin/env python3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import io
import pandas as pd
import sqlite3
import logging
import time
import json
from datetime import datetime, date, timedelta
import os

# Logging setup (similar to other scripts)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ftd_pull.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Config
DB_PATH = os.path.join('data', 'ftd_data.db')
MAX_MONTHS_BACK = 60  # For initial load, fetch last N months; increase cautiously for full history
MIN_START_DATE = date(2009, 7, 1)  # Half-month format starts July 2009; clamp to avoid pre-format files
os.makedirs('data', exist_ok=True)  # Ensure data dir

def init_db(drop_table=False):
    """Initialize FTD database table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if drop_table:
        logger.warning("Dropping table for fresh init (use sparingly).")
        cursor.execute('DROP TABLE IF EXISTS ftd_data')
    else:
        logger.info("Initializing FTD DB (no drop).")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ftd_data (
        date TEXT NOT NULL,
        cusip TEXT,
        symbol TEXT NOT NULL,
        quantity INTEGER,
        description TEXT,
        price REAL,
        source TEXT NOT NULL,
        ingest_timestamp TEXT NOT NULL,
        UNIQUE(date, symbol)
    )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ftd_date_symbol ON ftd_data (date, symbol)')
    conn.commit()
    conn.close()
    logger.info("FTD DB initialized.")

def get_last_date(table: str, conn) -> str:
    """Get max date from table."""
    df = pd.read_sql(f"SELECT MAX(date) FROM {table}", conn)
    last = df.iloc[0, 0] if not df.empty and pd.notna(df.iloc[0, 0]) else '1900-01-01'
    logger.debug(f"Last date in {table}: {last}")
    return last

def insert_data(df: pd.DataFrame, table: str, conn):
    """Insert DataFrame to table in chunks to avoid SQLite variable limit."""
    if df.empty:
        logger.warning(f"Empty DataFrame for {table}, skipping insert.")
        return 0
    try:
        pre_count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
        chunk_size = 1000  # Increased for faster inserts (still safe under 999 vars)
        total_attempted = len(df)
        num_chunks = (total_attempted + chunk_size - 1) // chunk_size
        for i in range(0, total_attempted, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunk.to_sql(table, conn, if_exists='append', index=False, method=None)  # method=None for speed on small chunks
            logger.debug(f"Inserted chunk {i//chunk_size + 1}/{num_chunks} ({len(chunk)} rows)")
        post_count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
        actual_inserted = post_count - pre_count
        logger.info(f"{table}: Inserted {actual_inserted} rows (attempted {total_attempted} in {num_chunks} chunks).")
        conn.commit()
        return actual_inserted
    except Exception as e:
        logger.error(f"Insert failed for {table}: {e}")
        conn.rollback()
        return 0

def get_half_months(start_dt: date, end_dt: date) -> list:
    """Generate list of half-month identifiers (e.g., '202509a') to cover from start_dt to end_dt."""
    half_months = []
    current = max(start_dt, MIN_START_DATE)  # Ensure we don't go pre-format
    while current <= end_dt:
        year = current.year
        month = current.month
        day = current.day
        if day <= 15:
            half = 'a'
        else:
            half = 'b'
        half_months.append(f"{year}{month:02d}{half}")
        # Advance to next half-month
        if half == 'a':
            next_day = 16
            next_month = month
            next_year = year
        else:
            next_month = month + 1
            next_year = year
            if next_month > 12:
                next_month = 1
                next_year += 1
            next_day = 1
        current = date(next_year, next_month, next_day)
    return sorted(list(set(half_months)))  # Dedup and sort

def create_session():
    """Create a requests session with headers to avoid 403 errors."""
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept': 'application/zip,application/octet-stream,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
    })
    return session

def fetch_and_parse_ftd(half_month: str, session) -> pd.DataFrame:
    """Download and parse a single half-month FTD ZIP file using session."""
    url = f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{half_month}.zip"
    content = None
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.content  # Save bytes once
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            if not z.namelist():
                raise ValueError("Empty ZIP")
            file_name = z.namelist()[0]
            with z.open(file_name) as f:
                # Try encodings: utf-8-sig first (BOM), then utf-8, then latin-1
                encodings = ['utf-8-sig', 'utf-8', 'latin-1']
                df = None
                for enc in encodings:
                    try:
                        csv_io = io.StringIO(f.read().decode(enc))
                        df = pd.read_csv(
                            csv_io,
                            sep='|',
                            header=None,
                            names=['settlement_date', 'cusip', 'symbol', 'quantity', 'description', 'price'],
                            dtype=str,
                            on_bad_lines='skip'
                        )
                        logger.debug(f"Successfully read {half_month} with {enc}")
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        continue
                if df is None:
                    raise ValueError("All encodings failed")
        if df.empty:
            return df
        # Convert date
        df['date'] = pd.to_datetime(df['settlement_date'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        df = df.dropna(subset=['date'])  # Drop invalid dates
        # Clean columns
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['symbol'] = df['symbol'].str.strip()
        df['cusip'] = df['cusip'].str.strip()
        df['description'] = df['description'].str.strip()
        # Drop rows without symbol
        df = df.dropna(subset=['symbol'])
        # Drop duplicates within file (unlikely but safe)
        df = df.drop_duplicates(subset=['date', 'symbol'])
        logger.info(f"Parsed {len(df)} rows from {half_month} (date range: {df['date'].min()} to {df['date'].max()})")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch/parse {half_month}: {e}")
        return pd.DataFrame()

if __name__ == '__main__':
    start_time = time.time()
    errors = []
    total_inserted = 0
    notes = ""
    init_db(drop_table=True)  # Set to True for fresh start / backfill
    conn = sqlite3.connect(DB_PATH)
    last_date = get_last_date('ftd_data', conn)
    logger.info(f"Last date in DB: {last_date}")
    today = date.today()
    if last_date == '1900-01-01':
        # Initial load: last MAX_MONTHS_BACK months
        start_dt = today - timedelta(days=30 * MAX_MONTHS_BACK)
        start_dt = max(start_dt, MIN_START_DATE)  # Clamp to SEC format start
        notes = f"Initial load from ~{start_dt.strftime('%Y-%m')}"
    else:
        start_dt = (pd.to_datetime(last_date) + timedelta(days=1)).date()
    half_months = get_half_months(start_dt, today)
    logger.info(f"Fetching {len(half_months)} half-months: {half_months[:5]}...{half_months[-5:]}")  # Truncated log for long lists
    session = create_session()
    processed_files = 0
    for i, hm in enumerate(half_months, 1):
        df = fetch_and_parse_ftd(hm, session)
        if df.empty:
            continue
        # Filter to new dates only
        df_new = df[df['date'] > last_date].copy()
        if df_new.empty:
            logger.info(f"No new data in {hm} (all dates <= {last_date})")
            continue
        # Add metadata
        df_new['source'] = 'sec_ftd'
        df_new['ingest_timestamp'] = datetime.now().isoformat()
        # Select columns
        df_new = df_new[['date', 'cusip', 'symbol', 'quantity', 'description', 'price', 'source', 'ingest_timestamp']]
        # Insert and get actual count
        inserted = insert_data(df_new, 'ftd_data', conn)
        total_inserted += inserted
        if inserted == 0:
            errors.append(f"Insert failed for {hm} (0 rows inserted)")
        last_date = max(last_date, df_new['date'].max())  # Update for next filter
        processed_files += 1
        if i % 10 == 0:  # Progress every 10 files
            logger.info(f"Progress: {i}/{len(half_months)} files processed, {total_inserted:,} rows so far")
        time.sleep(0.3)  # Slightly faster rate limit
    session.close()
    conn.close()
    duration = time.time() - start_time
    status = 'success' if total_inserted > 0 and not errors else ('warning' if errors else 'error')
    notes += f" | Processed {processed_files}/{len(half_months)} files"
    if errors:
        notes += f" | Errors: {len(errors)}"
    logger.info(f"Total new rows inserted: {total_inserted:,}")
    from cron_logger import log_job_summary
    log_job_summary('SEC FTD Pull', status, total_inserted, errors, duration, notes)
