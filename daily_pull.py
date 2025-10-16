import json
import sqlite3
import pandas as pd
from occ_series_fetcher import OCCSeriesFetcher  # Import subclasses here or use dynamic import
# Add other fetchers as needed: from polygon_fetcher import PolygonOptionsFetcher

# Load config
with open('sources.json', 'r') as f:
    sources = json.load(f)

conn = sqlite3.connect('gme_data.db')
from database import init_db, get_last_date, insert_data
init_db()

for src in sources:
    fetcher_class = globals()[src['fetcher']]  # Dynamic dispatch (Camply-style)
    fetcher = fetcher_class()
    df = fetcher.fetch(src['params'])
    
    if df is not None and not df.empty:
        df = fetcher.normalize(df, src['name'])
        df['ticker'] = src['params'].get('symbol', 'GME')  # Add ticker
        
        last_date = get_last_date(src['table'], conn)
        if pd.to_datetime(df['date'].max()) > pd.to_datetime(last_date):
            insert_data(df, src['table'], conn)
            print(f"{src['name']}: {len(df)} new rows inserted.")
        else:
            print(f"{src['name']}: No new data (max date {df['date'].max()} <= {last_date}).")
    else:
        print(f"{src['name']}: Empty data, skipped.")

conn.close()
print("Daily pull complete.")
