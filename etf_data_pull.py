import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import pandas as pd
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# List of tickers (deduplicated)
tickers = list(set([
    'XRT', 'IWM', 'IJH', 'VTI', 'VXF', 'VBR', 'GMEU', 'GMEY',
    'IGME', 'FNDA', 'IWB'
]))

data = []
today = pd.Timestamp.now().date().isoformat()

# Optional: Add proxy (format: {'https': 'http://user:pass@ip:port'} or {'https': 'http://ip:port'} for no-auth)
proxies = {}  # e.g., {'https': 'http://proxy.example.com:8080'}

session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
})

for ticker in tickers:
    url = f'https://marketchameleon.com/Overview/{ticker}/ETFProfile'
    try:
        response = session.get(url, timeout=30, proxies=proxies)
        logger.info(f"Status code for {ticker}: {response.status_code}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Precise selector for table
        label_td = soup.find('td', string=re.compile(r'Shares Outstanding:\s*$', re.IGNORECASE))
        if label_td:
            value_td = label_td.find_next_sibling('td')
            if value_td:
                shares_outstanding = value_td.text.strip().replace(',', '')
                data.append({'date': today, 'ticker': ticker, 'shares_outstanding': shares_outstanding})
                logger.info(f"Successfully fetched data for {ticker}: {shares_outstanding}")
                time.sleep(2)
                continue

        # Fallback regex
        page_text = soup.get_text()
        match = re.search(r'Shares Outstanding:\s*([\d,]+)', page_text, re.IGNORECASE)
        if match:
            shares_outstanding = match.group(1).replace(',', '')
            data.append({'date': today, 'ticker': ticker, 'shares_outstanding': shares_outstanding})
            logger.info(f"Successfully fetched data for {ticker}: {shares_outstanding}")
        else:
            logger.warning(f"No data found for {ticker}")

    except Exception as e:
        logger.error(f"Error for {ticker}: {e}")
    time.sleep(2)

session.close()

if data:
    df = pd.DataFrame(data)
    print(df)
else:
    print("No data collected")

