#!/usr/bin/env python3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import pandas as pd
import logging
import time
import sqlite3
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etf_pull.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Start timer
start_time = time.time()

# Config (match daily_pull.py style; adjust as needed)
DB_PATH = os.path.join('data', 'etf_data.db')  # New DB for ETFs; synced via Syncthing to Windows (read-only there)
TICKERS = list(set([
    'XRT', 'IWM', 'IJH', 'VTI', 'VXF', 'VBR', 'GMEU', 'GMEY',
    'IGME', 'FNDA', 'IWB'
]))
TODAY = datetime.now().date().isoformat()  # YYYY-MM-DD for daily idempotency
# Optional: Add proxy (format: {'https': 'http://user:pass@ip:port'} or {'https': 'http://ip:port'} for no-auth)
PROXIES = {}  # e.g., {'https': 'http://proxy.example.com:8080'}

def fetch_etf_data(tickers, proxies=PROXIES):
    """Fetch shares outstanding like original; returns list of dicts."""
    data = []
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    })
    for ticker in tickers:
        url = f'https://marketchameleon.com/Overview/{ticker}/ETFProfile'
        try:
            response = session.get(url, timeout=30, proxies=proxies)
            logger.info(f"Status code for {ticker}: {response.status_code}")
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            # Precise selector for table
            label_td = soup.find('td', string=re.compile(r'Shares Outstanding:\s*$', re.IGNORECASE))
            if label_td:
                value_td = label_td.find_next_sibling('td')
                if value_td:
                    shares_outstanding = value_td.text.strip().replace(',', '')
                    data.append({'date': TODAY, 'ticker': ticker, 'shares_outstanding': shares_outstanding})
                    logger.info(f"Successfully fetched data for {ticker}: {shares_outstanding}")
                    time.sleep(2)
                    continue
            # Fallback regex
            page_text = soup.get_text()
            match = re.search(r'Shares Outstanding:\s*([\d,]+)', page_text, re.IGNORECASE)
            if match:
                shares_outstanding = match.group(1).replace(',', '')
                data.append({'date': TODAY, 'ticker': ticker, 'shares_outstanding': shares_outstanding})
                logger.info(f"Successfully fetched data for {ticker}: {shares_outstanding}")
            else:
                logger.warning(f"No data found for {ticker}")
        except Exception as e:
            logger.error(f"Error for {ticker}: {e}")
        time.sleep(2)  # Rate limit
    session.close()
    return data

def normalize_data(raw_data):
    """Normalize like daily_pull.py: handle types, drop invalids, add source."""
    if not raw_data:
        return pd.DataFrame()
    df = pd.DataFrame(raw_data)
    df['shares_outstanding'] = pd.to_numeric(df['shares_outstanding'], errors='coerce').fillna(0).astype(int)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')  # Ensure string format
    df['source'] = 'marketchameleon'  # Track origin
    df = df[df['date'] == TODAY]  # Only today's data for daily insert
    df = df[['date', 'ticker', 'shares_outstanding', 'source']]  # Order columns
    return df

def upsert_to_db(df, db_path=DB_PATH):
    """Upsert to SQLite like daily_pull.py: idempotent on (date, ticker)."""
    if df.empty:
        logger.info("No new data to insert.")
        return
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create table if not exists (schema: date TEXT, ticker TEXT, shares_outstanding INTEGER, source TEXT)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_etf_shares (
            date TEXT,
            ticker TEXT,
            shares_outstanding INTEGER,
            source TEXT,
            PRIMARY KEY (date, ticker)
        )
    ''')
    # Upsert via Pandas (append is safe with PRIMARY KEY conflict resolution)
    df.to_sql('daily_etf_shares', conn, if_exists='append', index=False, method='multi')
    conn.commit()
    conn.close()
    logger.info(f"Upserted {len(df)} rows for {TODAY}")

if __name__ == '__main__':
    errors = []
    try:
        raw_data = fetch_etf_data(TICKERS)
        normalized_df = normalize_data(raw_data)
        print(normalized_df)  # Keep your original print for quick check
        upsert_to_db(normalized_df)
        rows_inserted = len(normalized_df)
        status = 'success' if rows_inserted > 0 else 'warning'
        notes = f"Fetched {len(TICKERS)} tickers"
    except Exception as e:
        logger.error(f"Main flow error: {e}")
        errors = [str(e)]
        status = 'error'
        rows_inserted = 0
        notes = "Failed to fetch/insert"

    duration = time.time() - start_time
    from cron_logger import log_job_summary
    log_job_summary('ETF Data Pull', status, rows_inserted, errors, duration, notes)
