import requests
class OCCSeriesFetcher(BaseFetcher):
    def fetch(self, params):
        url = f"https://marketdata.theocc.com/series-search?symbolType=U&symbol={params['symbol']}"
        resp = requests.get(url, timeout=10)
        # Assume response triggers TXT download; parse content or follow redirect
        if 'txt' in resp.headers.get('content-type', ''):
            df = pd.read_csv(pd.StringIO(resp.text), sep='|', header=None)  # Pipe-delimited
            df.columns = ['OCCSymbol', 'Underlying', 'PutCall', 'Strike', 'Expiry', 'OI', 'Volume']  # Adapt from spec
            df['date'] = datetime.now().date()
            return df
        return None
