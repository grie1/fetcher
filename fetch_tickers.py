import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import sqlite3

# Project paths (relative to ~/Desktop/github/fetcher)
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "fetcher_data.db")

# Create data dir if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

# Configurable list of tickers (expand here: add 'AMC', etc.)
TICKERS = ["GME", "KOSS"]  # Start with these; easy to add more

# Full historical period (fallback if no data for a ticker)
end_date = datetime.now()  # Today (e.g., 2025-11-05)
start_date_full = end_date - timedelta(days=365*10)  # ~10 years ago

# Connect to DB (create if missing)
conn = sqlite3.connect(DB_PATH)

# Drop and recreate table for clean schema (idempotent; comment out after first run)
conn.execute("DROP TABLE IF EXISTS historical")
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

total_new_rows = 0
for ticker in TICKERS:
    print(f"\n--- Processing {ticker} ---")
    
    # Get last date for this ticker
    last_row = pd.read_sql("SELECT MAX(Date) as last_date FROM historical WHERE ticker=?", conn, params=(ticker,))
    last_date = None
    if not last_row.empty and pd.notna(last_row['last_date'].iloc[0]):
        last_date = pd.to_datetime(last_row['last_date'].iloc[0]).date()
        print(f"Last DB date for {ticker}: {last_date}. Fetching from next day.")
        start_date = (pd.to_datetime(last_date) + timedelta(days=1)).date()
    else:
        print(f"No data for {ticker}. Fetching full 10 years.")
        start_date = start_date_full.date()
    
    # Fetch data (incremental or full; auto_adjust=False for Adj Close)
    data_new = yf.download(ticker, start=start_date, end=end_date, interval="1d", auto_adjust=False)
    
    if data_new.empty:
        print(f"No new data for {ticker} today.")
        continue
    
    # Flatten MultiIndex columns: Take level 0 (attributes: 'Open', 'High', etc.)
    if isinstance(data_new.columns, pd.MultiIndex):
        data_new.columns = data_new.columns.get_level_values(0)
    
    # Add ticker column
    data_new['ticker'] = ticker
    
    # Reset index to 'Date' column
    data_new = data_new.reset_index()
    
    # Explicitly select/reorder columns to match schema (drops junk like 'Price')
    data_new = data_new[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'ticker']]
    
    print(data_new.head())  # Preview new rows
    print(f"New data for {ticker}: {data_new.shape} (rows x columns)")
    print(f"New date range: {data_new['Date'].dt.date.min()} to {data_new['Date'].dt.date.max()}")
    
    # Append to DB
    data_new.to_sql("historical", conn, if_exists="append", index=False)
    new_rows = len(data_new)
    total_new_rows += new_rows
    print(f"Appended {new_rows} new rows for {ticker}.")

conn.close()
print(f"\n--- Summary: Appended {total_new_rows} total new rows to {DB_PATH} ---")
print("Ready for Syncthing sync to Windows!")
