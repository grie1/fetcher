import time
import logging # Add
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import sqlite3
# Optional logging (replaces prints)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.FileHandler('logs/fetcher_cron.log', mode='a')])
logger = logging.getLogger(__name__)
# Start timer
start_time = time.time()
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "stock_ticker_data.db")  # Updated DB name
os.makedirs(DATA_DIR, exist_ok=True)
TICKERS = ["GME", "KOSS"]
end_date = datetime.now()
start_date_full = end_date - timedelta(days=365*10)
conn = sqlite3.connect(DB_PATH)
conn.execute("DROP TABLE IF EXISTS historical") # Comment out after first run
conn.execute("""
CREATE TABLE historical (
    Date TEXT,
    ticker TEXT NOT NULL,
    Open REAL,
    High REAL,
    Low REAL,
    Close REAL,
    "Adj Close" REAL,
    Volume INTEGER,
    PRIMARY KEY (Date, ticker)
)
""")
conn.commit()
errors = []
total_new_rows = 0
for ticker in TICKERS:
    try:
        logger.info(f"\n--- Processing {ticker} ---")
        last_row = pd.read_sql("SELECT MAX(Date) as last_date FROM historical WHERE ticker=?", conn, params=(ticker,))
        if not last_row.empty and pd.notna(last_row['last_date'].iloc[0]):
            last_date = pd.to_datetime(last_row['last_date'].iloc[0]).date()
            logger.info(f"Last DB date for {ticker}: {last_date}. Fetching from next day.")
            start_date = (pd.to_datetime(last_date) + timedelta(days=1)).date()
        else:
            logger.info(f"No data for {ticker}. Fetching full 10 years.")
            start_date = start_date_full.date()
        data_new = yf.download(ticker, start=start_date, end=end_date, interval="1d", auto_adjust=False)
        if data_new.empty:
            logger.info(f"No new data for {ticker} today.")
            continue
        if isinstance(data_new.columns, pd.MultiIndex):
            data_new.columns = data_new.columns.get_level_values(0)
        data_new['ticker'] = ticker
        data_new = data_new.reset_index()
        data_new = data_new[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'ticker']]
        logger.info(data_new.head().to_string()) # Preview
        logger.info(f"New data for {ticker}: {data_new.shape} (rows x columns)")
        logger.info(f"New date range: {data_new['Date'].dt.date.min()} to {data_new['Date'].dt.date.max()}")
        data_new.to_sql("historical", conn, if_exists="append", index=False)
        new_rows = len(data_new)
        total_new_rows += new_rows
        logger.info(f"Appended {new_rows} new rows for {ticker}.")
    except Exception as e:
        error_msg = f"{ticker}: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
conn.close()
duration = time.time() - start_time
status = 'success' if total_new_rows > 0 and not errors else ('warning' if errors else 'error')
notes = f"Updated {len(TICKERS)} tickers"
logger.info(f"\n--- Summary: Appended {total_new_rows} total new rows to {DB_PATH} ---")
logger.info("Ready for Syncthing sync to Windows!")
from cron_logger import log_job_summary
log_job_summary('Ticker Historical Data', status, total_new_rows, errors, duration, notes)
