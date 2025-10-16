import requests
import pandas as pd
from io import StringIO
from base_fetcher import BaseFetcher
from datetime import datetime

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
        
        content_type = resp.headers.get('content-type', '').lower()
        if 'octet-stream' not in content_type and 'text/plain' not in content_type and 'txt' not in content_type:
            print(f"Unexpected content-type: {content_type}")
            return pd.DataFrame()
        
        # Robust line-by-line parse: Filter for 11-field data lines
        try:
            lines = resp.text.splitlines()
            data_rows = []
            for line in lines:
                fields = line.split()  # Default split() uses whitespace
                if len(fields) == 11 and fields[0] in ['GME', 'GME1']:  # Data pattern for GME
                    data_rows.append(fields)
            
            if not data_rows:
                print("No data rows found.")
                return pd.DataFrame()
            
            # Build DF with columns
            df_raw = pd.DataFrame(data_rows, columns=[
                'ProductSymbol', 'year', 'Month', 'Day', 'Integer', 'Dec', 
                'C_indicator', 'P_indicator', 'call_oi', 'put_oi', 'limit'
            ])
            
            # Clean: Numeric types
            df_raw['Integer'] = pd.to_numeric(df_raw['Integer'], errors='coerce')
            df_raw['Dec'] = df_raw['Dec'].astype(str).str.zfill(3)  # '000'
            df_raw['call_oi'] = pd.to_numeric(df_raw['call_oi'], errors='coerce').fillna(0).astype(int)
            df_raw['put_oi'] = pd.to_numeric(df_raw['put_oi'], errors='coerce').fillna(0).astype(int)
            df_raw['limit'] = pd.to_numeric(df_raw['limit'], errors='coerce').fillna(0).astype(int)
            
            # Drop invalid
            df_raw = df_raw.dropna(subset=['ProductSymbol'])
            
            # Compute strike and expiry
            df_raw['strike_price'] = df_raw['Integer'] + pd.to_numeric('0.' + df_raw['Dec'])
            df_raw['expiration_date'] = (
                df_raw['year'].astype(str) + '-' + 
                df_raw['Month'].astype(str).str.zfill(2) + '-' + 
                df_raw['Day'].astype(str).str.zfill(2)
            )
            
            # Duplicate for calls/puts (only if OI >0)
            calls = df_raw[df_raw['call_oi'] > 0].copy()
            if not calls.empty:
                calls['put_call'] = 'C'
                calls['open_interest'] = calls['call_oi']
                calls['contract_symbol'] = (
                    calls['ProductSymbol'].astype(str) + 
                    calls['year'].astype(str) + 
                    calls['Month'].astype(str).str.zfill(2) + 
                    calls['Day'].astype(str).str.zfill(2) + 'C' + 
                    calls['Integer'].astype(str).str.zfill(5) + calls['Dec'] + '00'
                )
            
            puts = df_raw[df_raw['put_oi'] > 0].copy()
            if not puts.empty:
                puts['put_call'] = 'P'
                puts['open_interest'] = puts['put_oi']
                puts['contract_symbol'] = (
                    puts['ProductSymbol'].astype(str) + 
                    puts['year'].astype(str) + 
                    puts['Month'].astype(str).str.zfill(2) + 
                    puts['Day'].astype(str).str.zfill(2) + 'P' + 
                    puts['Integer'].astype(str).str.zfill(5) + puts['Dec'] + '00'
                )
            
            df = pd.concat([calls, puts], ignore_index=True)
            if df.empty:
                return pd.DataFrame()
            
            # Add metadata (ticker added in daily_pull.py)
            df['date'] = datetime.now().strftime('%Y-%m-%d')
            df['volume'] = 0  # Placeholder; merge Polygon for volume/prices
            df['last_price'] = 0.0
            df['bid'] = 0.0
            df['ask'] = 0.0
            df['source'] = 'OCC'
            
            # Select schema columns (date first, source last; ticker post-fetch)
            df = df[[
                'date', 'contract_symbol', 'put_call', 'strike_price', 'expiration_date', 
                'open_interest', 'volume', 'last_price', 'bid', 'ask', 'source'
            ]]
            
            print(f"Fetched {len(df)} series for {symbol}.")
            return df
            
        except Exception as e:
            print(f"Parse error: {e}")
            return pd.DataFrame()
