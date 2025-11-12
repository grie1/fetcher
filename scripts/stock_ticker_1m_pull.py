import time
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone  # Added timezone
import os
import sqlite3
import sys  # For StreamHandler

# Suppress yfinance logging
logging.getLogger('yfinance').setLevel(logging.WARNING)

# Logging setup: Manual handlers only (no basicConfig to avoid dupes)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler (shared cron log)
file_handler = logging.FileHandler('logs/fetcher_cron.log', mode='a')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Console handler (for cron >> per-script log)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Start timer
start_time = time.time()
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "stock_ticker_1m_data.db")
os.makedirs(DATA_DIR, exist_ok=True)
TICKERS = ["GME", "KOSS"]
now_utc = datetime.now(timezone.utc)  # UTC-aware "now"
conn = sqlite3.connect(DB_PATH)
# conn.execute("DROP TABLE IF EXISTS historical_1m")  # Comment out after first successful run!
conn.execute("""
CREATE TABLE IF NOT EXISTS historical_1m (
    Datetime TEXT,
    ticker TEXT NOT NULL,
    Open REAL,
    High REAL,
    Low REAL,
    Close REAL,
    Volume INTEGER,
    PRIMARY KEY (Datetime, ticker)
)
""")
conn.commit()

# One-time index creation (safe to run every time; placed BEFORE any data processing)
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_ticker_datetime';")
if not cursor.fetchone():
    logger.info("Creating index idx_ticker_datetime for query performance...")
    cursor.execute("CREATE INDEX idx_ticker_datetime ON historical_1m(ticker, Datetime);")
    conn.commit()
    logger.info("Index created successfully.")
cursor.close()  # Clean up cursor

errors = []
total_new_rows = 0
for ticker in TICKERS:
    try:
        logger.info(f"\n--- Processing {ticker} ---")
        last_row = pd.read_sql("SELECT MAX(Datetime) as last_date FROM historical_1m WHERE ticker=?", conn, params=(ticker,))
        if not last_row.empty and pd.notna(last_row['last_date'].iloc[0]):
            # Ensure scalar extraction
            last_date_scalar = last_row['last_date'].iloc[0]
            last_dt = pd.to_datetime(last_date_scalar, utc=True)
            start_dt = last_dt + timedelta(minutes=1)
            logger.info(f"Last DB datetime for {ticker}: {last_dt}. Fetching from next minute.")
            fetch_method = "start_end"
        else:
            logger.info(f"No data for {ticker}. Fetching last 7 days via period='7d' (API-safe).")
            fetch_method = "period"
        logger.info(f"Current UTC time: {now_utc}")
        if fetch_method == "period":
            data_new = yf.download(ticker, period="7d", interval="1m", auto_adjust=False, progress=False)
        else:
            # Cap to avoid API limits
            delta = (now_utc - start_dt).days
            if delta > 7:
                start_dt = now_utc - timedelta(days=7)
                logger.info(f"Capping fetch for {ticker} to last 7 days due to API limits.")
            # If start > now, no new data—skip fetch
            if start_dt > now_utc:
                logger.info(f"Start ({start_dt}) after now ({now_utc}) for {ticker}—no new data.")
                continue
            logger.info(f"Fetching data for {ticker} from {start_dt} to {now_utc}...")
            data_new = yf.download(ticker, start=start_dt, end=now_utc, interval="1m", auto_adjust=False, progress=False)
        
        logger.info(f"yfinance returned {len(data_new)} raw rows for {ticker}.")
        if data_new.empty:
            logger.warning(f"No data returned for {ticker}—check trading days or API availability.")
            continue
        
        if isinstance(data_new.columns, pd.MultiIndex):
            data_new.columns = data_new.columns.get_level_values(0)
        data_new['ticker'] = ticker
        data_new = data_new.reset_index()
        logger.info(f"Processed shape for {ticker}: {data_new.shape}")
        logger.info(f"Raw datetime range: {data_new['Datetime'].min()} to {data_new['Datetime'].max()}")
        logger.info(data_new.head(3).to_string())  # Short preview
        
        # Format for DB: Convert to UTC string (drops tz, but parse back with utc=True)
        data_new['Datetime'] = data_new['Datetime'].dt.tz_convert('UTC').dt.strftime('%Y-%m-%d %H:%M:%S')
        data_new = data_new[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']]
        
        logger.info(f"Inserting {len(data_new)} rows into DB...")
        data_new.to_sql("historical_1m", conn, if_exists="append", index=False)
        new_rows = len(data_new)
        total_new_rows += new_rows
        logger.info(f"Appended {new_rows} new rows for {ticker}.")
    except Exception as e:
        error_msg = f"{ticker}: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)

conn.close()  # Close only after everything (including index if needed)
duration = time.time() - start_time
status = 'success' if total_new_rows > 0 and not errors else ('warning' if errors else 'error')
notes = f"Updated {len(TICKERS)} tickers"
logger.info(f"\n--- Summary: Appended {total_new_rows} total new rows to {DB_PATH} ---")
logger.info("Ready for Syncthing sync to Windows!")
try:
    from cron_logger import log_job_summary
    log_job_summary('1m Ticker Intraday Data', status, total_new_rows, errors, duration, notes)
except ImportError:
    logger.warning("cron_logger not found—skipping summary log.")
