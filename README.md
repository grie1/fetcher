Fetcher

fetcher/
├── scripts/          # Core Python modules
│   ├── base_fetcher.py     # ABC for fetch/normalize
│   ├── occ_series_fetcher.py # OCC scraper
│   ├── etf_data_pull.py    # MarketChameleon ETF pull
│   ├── fetch_tickers.py    # yfinance historical
│   ├── market_holidays.py  # Polygon holidays + cache
│   ├── occ_options_pull.py # Main OCC runner (ex-daily_pull)
│   ├── database.py         # SQLite utils (init/upsert/get_last_date)
│   ├── cron_logger.py      # Job summary DB
│   └── send_daily_summary.py # Mailgun HTML emails
├── data/             # Persistent storage (Syncthing sync)
│   ├── gme_data.db        # Options series (OCC)
│   ├── etf_data.db        # ETF shares (daily_etf_shares table)
│   ├── fetcher_data.db    # Ticker historical (historical table)
│   ├── cron_logs.db       # Job metrics (job_logs table)
│   ├── market_holidays.json # Cached holidays
│   └── ETF_HISTORICAL_DATA.csv # One-off import (optional)
├── logs/             # Rotated outputs (logrotate weekly)
│   ├── occ_options_pull.log
│   ├── etf_pull.log
│   ├── fetcher_cron.log
│   ├── holidays.log
│   └── summary.log
├── config/          # (Optional future: cron_config.yaml)
│   └── sources.json # OCC params (symbol, table, fetcher)
├── .env             # Secrets (gitignored)
├── requirements.txt # Dependencies
└── README.md

Flow: Cron → Script (fetch/normalize via BaseFetcher) → SQLite upsert (idempotent) → cron_logger entry → EOD summary queries DB → Mailgun email.
Error Handling: Try/except per source, logged to file + cron_logs.db. Non-trading days skip pulls.
Tech Stack:

Core: Python 3.8+, pandas (DataFrames), sqlite3 (DB).
Fetch: requests (HTTP), BeautifulSoup (scraping), yfinance (tickers), polygon (holidays).
Utils: tenacity (retries), logging (file/console), python-dotenv (.env).
Email: requests + Mailgun API (HTML tables with inline CSS).

.env
MAILGUN_DOMAIN=your-sandbox.mailgun.org
MAILGUN_API_KEY=your-api-key
SUMMARY_EMAIL=your@email.com
POLYGON_API_KEY=your-polygon-key  # Free tier OK
DROP_TABLE=false  # Set true for fresh DB init (one-time)

One-Time Setup:

Init DBs: Run python scripts/occ_options_pull.py (creates gme_data.db, etc.).
Prime holidays: python scripts/market_holidays.py (fetches 2025/2026, caches JSON).
Import ETF historical (optional): python scripts/import_etf_data.py (multiplies CSV values, upserts to etf_data.db).
Syncthing: Point data/ to your Windows folder for auto-sync.


Load Sources (sources.json—expandable):
[
  {
    "name": "OCC_GME_Series",
    "fetcher": "OCCSeriesFetcher",
    "params": { "symbol": "GME" },
    "table": "options_data"
  }
  // Add: { "name": "Polygon_Volume", "fetcher": "PolygonFetcher", ... }
]

cron:
Group,Schedule,Command,Purpose
Daily,3:00 AM,occ_options_pull.py → logs/occ_options_pull.log,OCC options (trading days only)
,6:00 PM,etf_data_pull.py → logs/etf_pull.log,ETF shares outstanding
,3:00 AM,fetch_tickers.py → logs/fetcher_cron.log,Incremental ticker historical
,11:00 PM,send_daily_summary.py → logs/summary.log,EOD email summary
Intra-Day,Every 4h,"occ_options_pull.py (JOB_NAME=""OCC Frequent Check"") → logs/...",OCC real-time checks
Weekly,Sun 00:00,market_holidays.py → logs/holidays.log,Update holidays cache
