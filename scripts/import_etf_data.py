#!/usr/bin/env python3
"""
Import historical ETF data from CSV into the daily_etf_shares table.
Determines per-ticker multiplier (1 or 1000) based on reference date 2025-11-04
by comparing CSV value to existing DB value on that date.
Assumes DB has data for 2025-11-04 from the pull script.
Handles NaN/empty values by dropping them.
Filters out existing (date, ticker) before insert for idempotency.
"""

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/etf_import.log', mode='a')]
)

# Config
CSV_PATH = 'ETF_HISTORICAL_DATA.csv'
DB_PATH = 'data/etf_data.db'
SOURCE = 'marketchameleon'
REF_DATE_CSV = '11/4/2025'  # Format in CSV
REF_DATE_DB = '2025-11-04'  # Normalized format
TICKERS = ['XRT', 'FNDA', 'IWB', 'IWM', 'IJH', 'VTI', 'VXF', 'VBR', 'GMEU', 'GMEY', 'IGME']

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_historical_data(csv_path, db_path, source):
    """Read CSV, determine multipliers from ref date, normalize, filter new rows, and upsert to DB."""
    conn = sqlite3.connect(db_path)
    
    # Query DB for reference values
    ref_db_df = pd.read_sql_query(
        "SELECT ticker, shares_outstanding FROM daily_etf_shares WHERE date=?", 
        conn, params=(REF_DATE_DB,)
    )
    ref_db = ref_db_df.set_index('ticker')['shares_outstanding'].to_dict()
    logger.info(f"Reference DB values loaded for {len(ref_db)} tickers on {REF_DATE_DB}")
    
    conn.close()
    
    if not ref_db:
        raise ValueError(f"No reference data found in DB for {REF_DATE_DB}. Run the pull script first.")
    
    # Read CSV: first col dates, header tickers (skip BOM/empty first col if needed)
    df = pd.read_csv(csv_path)
    # Rename first col to 'date' (it's unnamed due to leading empty/BOM)
    df.rename(columns={df.columns[0]: 'date'}, inplace=True)
    
    # Find reference row in CSV (before date normalization)
    ref_row_idx = df[df['date'] == REF_DATE_CSV].index
    if ref_row_idx.empty:
        raise ValueError(f"No reference row found in CSV for {REF_DATE_CSV}")
    ref_row = df.loc[ref_row_idx[0], TICKERS]  # Only tickers columns
    ref_csv = ref_row.to_dict()
    logger.info(f"Reference CSV values loaded for {len(ref_csv)} tickers on {REF_DATE_CSV}")
    
    # Compute multipliers
    multipliers = {}
    for ticker in TICKERS:
        if ticker not in ref_csv or pd.isna(ref_csv[ticker]) or ref_csv[ticker] == 0:
            logger.warning(f"Skipping multiplier for {ticker}: no CSV ref value")
            continue
        if ticker not in ref_db:
            logger.warning(f"Skipping multiplier for {ticker}: no DB ref value")
            continue
        csv_val = ref_csv[ticker]
        db_val = ref_db[ticker]
        mult = db_val / csv_val
        # Round to nearest int, expect 1 or 1000
        mult_rounded = round(mult)
        if mult_rounded not in [1, 1000]:
            logger.warning(f"Unexpected multiplier {mult:.2f} for {ticker}; using 1000")
            mult_rounded = 1000
        multipliers[ticker] = mult_rounded
        logger.info(f"Multiplier for {ticker}: {mult_rounded} (CSV: {csv_val}, DB: {db_val})")
    
    # Now normalize dates
    df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    
    # Melt to long format: date | ticker | shares_outstanding (raw)
    melted_df = df.melt(id_vars=['date'], var_name='ticker', value_name='shares_outstanding')
    melted_df = melted_df[melted_df['ticker'].isin(TICKERS)]  # Filter to known tickers
    
    # Drop rows with NaN/empty values
    melted_df = melted_df.dropna(subset=['shares_outstanding'])
    
    # Apply per-ticker multiplier
    def apply_mult(row):
        mult = multipliers.get(row['ticker'], 1000)
        return pd.to_numeric(row['shares_outstanding']) * mult
    melted_df['shares_outstanding'] = melted_df.apply(apply_mult, axis=1).astype(int)
    
    # Add source
    melted_df['source'] = source
    
    # Select/rename columns to match schema
    final_df = melted_df[['date', 'ticker', 'shares_outstanding', 'source']]
    
    # Filter out existing rows for idempotency
    conn = sqlite3.connect(db_path)
    existing_df = pd.read_sql_query("SELECT date, ticker FROM daily_etf_shares", conn)
    existing_keys = set(zip(existing_df['date'], existing_df['ticker']))
    conn.close()
    
    final_df['temp_key'] = list(zip(final_df['date'], final_df['ticker']))
    new_df = final_df[~final_df['temp_key'].isin(existing_keys)].drop('temp_key', axis=1)
    
    # Log preview
    logger.info(f"Prepared {len(final_df)} total rows; {len(new_df)} new rows for import.")
    if not new_df.empty:
        print(new_df.head(10))  # Quick check, more rows for variety
    else:
        logger.info("No new rows to insert.")
        return
    
    # Upsert to DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if not exists (matches etf_data_pull.py)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_etf_shares (
            date TEXT,
            ticker TEXT,
            shares_outstanding INTEGER,
            source TEXT,
            PRIMARY KEY (date, ticker)
        )
    ''')
    
    # Append new rows only (PRIMARY KEY handles any edge cases)
    new_df.to_sql('daily_etf_shares', conn, if_exists='append', index=False, method='multi')
    
    conn.commit()
    conn.close()
    
    # Verify insert count
    conn = sqlite3.connect(db_path)
    count = pd.read_sql_query("SELECT COUNT(*) as total FROM daily_etf_shares", conn).iloc[0]['total']
    logger.info(f"Database now has {count} total rows.")
    conn.close()

if __name__ == '__main__':
    start_time = time.time()
    errors = []
    try:
        import_historical_data(CSV_PATH, DB_PATH, SOURCE)
        # Assume you add: inserted_rows = len(new_df) in function and return it
        # For now, query post-import:
        conn = sqlite3.connect(DB_PATH)
        inserted_rows = pd.read_sql("SELECT COUNT(*) FROM daily_etf_shares", conn).iloc[0, 0]
        conn.close()
        status = 'success'
        notes = f"Imported for {len(TICKERS)} tickers"
    except Exception as e:
        errors = [str(e)]
        status = 'error'
        inserted_rows = 0
        notes = "Failed import"

    duration = time.time() - start_time
    from cron_logger import log_job_summary
    log_job_summary('ETF Historical Import', status, inserted_rows, errors, duration, notes)
