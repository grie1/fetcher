import pandas as pd
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt
from abc import ABC, abstractmethod
import sqlite3

class BaseFetcher(ABC):
    @abstractmethod
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch(self, params: dict) -> pd.DataFrame:
        pass

    def normalize(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        df['date'] = pd.to_datetime(df['date'] or datetime.now().date()).dt.strftime('%Y-%m-%d')
        df['source'] = source_name
        df['ingest_timestamp'] = datetime.now()
        return df

# Usage in daily_pull.py loop:
conn = sqlite3.connect('gme_data.db')
for src in sources:
    fetcher = globals()[src['fetcher']](**src['params'])
    df = fetcher.fetch()
    if df is not None and not df.empty:
        df = fetcher.normalize(df, src['name'])
        last_date = pd.read_sql(f"SELECT MAX(date) FROM {src['table']}", conn).iloc[0,0]
        if df['date'].max() > last_date:
            df.to_sql(src['table'] or 'gme_metrics', conn, if_exists='append', index=False)
            print(f"{src['name']}: {len(df)} new rows")
conn.close()
