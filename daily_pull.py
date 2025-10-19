import json
import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime, date
from market_holidays import is_trading_day

# Setup logging: Append to occ_daily.log, with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('occ_daily.log', mode='a'),  # Append mode
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# Import fetchers (unchanged)
from occ_series_fetcher import OCCSeriesFetcher
# Add others as needed, e.g., from polygon_fetcher import PolygonOptionsFetcher

# Load config (unchanged)
with open('sources.json', 'r') as f:
    sources = json.load(f)

# DB setup
conn = sqlite3.connect('gme_data.db')
from database import init_db, get_last_date, insert_data
drop_table = os.getenv('DROP_TABLE', 'false').lower() == 'true'
init_db(drop_table=drop_table)

logger.info(f"Daily pull started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Sources: {len(sources)} | Drop table: {drop_table}")

# Trading day check (skip non-trading days)
today_date = date.today()
if not is_trading_day(today_date):
    logger.info(f"Non-trading day ({today_date}): {today_date.strftime('%A')} or holiday. Skipping pull.")
    conn.close()
    logger.info(f"DROP_TABLE env: {os.getenv('DROP_TABLE', 'false')}")
    logger.info(f"Daily pull complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Total new rows across sources: 0")
    exit()

total_inserted = 0
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
        
        df = fetcher.normalize(df, src_name)  # Assumes normalize adds 'ingest_timestamp'
        df['ticker'] = src['params'].get('symbol', 'GME')
        
        # Delta check
        last_date = get_last_date(src['table'], conn)
        df_date = pd.to_datetime(df['date'].max()).strftime('%Y-%m-%d')
        last_date_str = pd.to_datetime(last_date).strftime('%Y-%m-%d') if last_date != '1900-01-01' else last_date
        
        logger.debug(f"{src_name}: DF max date '{df_date}' vs DB last '{last_date_str}'")
        
        if df_date > last_date_str:
            insert_data(df, src['table'], conn)
            # Fetch post-insert count for logging (insert_data already logs details)
            post_count = pd.read_sql(f"SELECT COUNT(*) FROM {src['table']}", conn).iloc[0, 0]
            logger.info(f"{src_name}: Insert complete | Total rows in table now: {post_count}")
            total_inserted += len(df)
        else:
            logger.info(f"{src_name}: No new data (max date {df_date} <= {last_date_str}), skipped.")
            
    except Exception as e:
        logger.error(f"{src_name}: Failed with error: {e}")

conn.close()
logger.info(f"DROP_TABLE env: {os.getenv('DROP_TABLE', 'false')}")
logger.info(f"Daily pull complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Total new rows across sources: {total_inserted}")
