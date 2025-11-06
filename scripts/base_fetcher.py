import pandas as pd
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt
from abc import ABC, abstractmethod

class BaseFetcher(ABC):
    @abstractmethod
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch(self, params: dict) -> pd.DataFrame:
        """Fetch raw data as DataFrame."""
        pass

    def normalize(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Normalize with shared fields."""
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df['source'] = source_name
        df['ingest_timestamp'] = datetime.now()
        return df
