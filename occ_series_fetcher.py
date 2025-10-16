import requests
import pandas as pd
from io import StringIO
from base_fetcher import BaseFetcher

class OCCSeriesFetcher(BaseFetcher):
    def fetch(self, params: dict) -> pd.DataFrame:
        symbol = params.get('symbol', 'GME')
        url = f"https://marketdata.theocc.com/series-search?symbolType=U&symbol={symbol}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"OCC fetch failed: {resp.status_code}")
            return pd.DataFrame()
        
        content_type = resp.headers.get('content-type', '')
        if 'text/plain' not in content_type and 'txt' not in content_type.lower():
            print("Response not TXT; possible redirect/HTML.")
            return pd.DataFrame()
        
        # Parse pipe-delimited TXT with header
        try:
            df = pd.read_csv(StringIO(resp.text), sep='|', header=0)
            # Columns from OCC spec (adapt if mismatch)
            expected_cols = ['OCCSymbol', 'Underlying', 'PutCall', 'Strike', 'Expiry', 'OI', 'Volume']
            if len(df.columns) < len(expected_cols):
                df.columns = expected_cols[:len(df.columns)]  # Truncate if fewer
            else:
                df = df[expected_cols]  # Select if more
            
            # Normalize types
            df['Strike'] = pd.to_numeric(df['Strike'], errors='coerce')
            df['OI'] = pd.to_numeric(df['OI'], errors='coerce').fillna(0).astype(int)
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype(int)
            df['Expiry'] = pd.to_datetime(df['Expiry'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Add date (snapshot day)
            df['date'] = datetime.now().date()
            
            # Drop invalid
            df = df.dropna(subset=['OCCSymbol'])
            
            print(f"Fetched {len(df)} series for {symbol}.")
            return df
        except Exception as e:
            print(f"Parse error: {e}")
            return pd.DataFrame()
