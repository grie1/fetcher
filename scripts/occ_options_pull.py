import time  # Already there, but ensure
import json
import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime, date
from market_holidays import is_trading_day  # Note: This imports the function; script is separate

# Robust logging setup (before any imports that configure logging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
root_logger = logging.getLogger()
root_logger.handlers.clear()  # Reset any prior configs (e.g., from imports)

file_handler = logging.FileHandler('logs/occ_options_pull.log', mode='a')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# Start timer early
start_time = time.time()

# Import fetchers
from market_holidays import is_trading_day
from occ_series_fetcher import OCCSeriesFetcher

# Load config
with open('sources.json', 'r') as f:
    sources = json.load(f)

# DB setup (fix paren)
conn = sqlite3.connect(os.path.join('data', 'gme_data.db'))  # Fixed
from database import init_db, get_last_date, insert_data
drop_table = os.getenv('DROP_TABLE', 'false').lower() == 'true'
init_db(drop_table=drop_table)
logger.info(f"Daily pull started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Sources: {len(sources)} | Drop table: {drop_table}")

# Trading day check
today_date = date.today()
errors = []  # Collect here
total_inserted = 0
if not is_trading_day(today_date):
    logger.info(f"Non-trading day ({today_date}): {today_date.strftime('%A')} or holiday. Skipping pull.")
    status = 'warning'
    notes = "Skipped: non-trading day"
else:
    status = 'error'  # Default to error if issues
    notes = f"Processed {len(sources)} sources"
    for src in sources:
        src_name = src['name']
        logger.info(f"Processing {src_name} | Params: {src['params']} | Table: {src['table']}")
        try:
            fetcher_class = globals()[src['fetcher']]
            fetcher = fetcher_class()
            df = fetcher.fetch(src['params'])
            if df is None or df.empty:
                logger.warning(f"{src_name}: Fetch returned empty/None, skipped.")
                continue
            df = fetcher.normalize(df, src_name)
            df['ticker'] = src['params'].get('symbol', 'GME')
            # Delta check
            last_date = get_last_date(src['table'], conn)
            df_date = pd.to_datetime(df['date'].max()).strftime('%Y-%m-%d')
            last_date_str = pd.to_datetime(last_date).strftime('%Y-%m-%d') if last_date != '1900-01-01' else last_date
            logger.debug(f"{src_name}: DF max date '{df_date}' vs DB last '{last_date_str}'")
            if df_date > last_date_str:
                insert_data(df, src['table'], conn)
                post_count = pd.read_sql(f"SELECT COUNT(*) FROM {src['table']}", conn).iloc[0, 0]
                logger.info(f"{src_name}: Insert complete | Total rows in table now: {post_count}")
                total_inserted += len(df)
            else:
                logger.info(f"{src_name}: No new data (max date {df_date} <= {last_date_str}), skipped.")
        except Exception as e:
            error_msg = f"{src_name}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
    status = 'success' if total_inserted > 0 and not errors else ('warning' if errors else 'error')

conn.close()
duration = time.time() - start_time
logger.info(f"DROP_TABLE env: {os.getenv('DROP_TABLE', 'false')}")
logger.info(f"Daily pull complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Total new rows across sources: {total_inserted}")

# Cron Logger (use env for frequent vs daily; default daily)
from cron_logger import log_job_summary
job_name = os.getenv('JOB_NAME', 'OCC Daily Pull')  # Set OCC Frequent Check in cron env if needed
log_job_summary(job_name, status, total_inserted, errors, duration, notes)
