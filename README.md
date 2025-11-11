# Fetcher README

## Project Overview
This repository contains scripts for scraping and storing financial data related to GME (GameStop) and related assets, including options series from OCC, ETF shares, FTD (Failures to Deliver), historical tickers, and market holidays. Data is stored in SQLite databases under the `data/` directory. Scripts are designed for cron-based daily pulls with logging and email summaries.

## Scripts Directory
The `scripts/` directory holds the core Python modules. Below is a synopsis of each script's purpose:

- **base_fetcher.py**: Defines an abstract base class (`BaseFetcher`) for standardizing data fetching and normalization across scrapers, ensuring consistent output DataFrames.

- **occ_series_fetcher.py**: Fetches options series data (e.g., strikes, open interest) for GME from the Options Clearing Corporation (OCC) API; parses raw text responses into structured DataFrames with calls/puts separated.

- **etf_data_pull.py**: Pulls ETF shares outstanding and related data from MarketChameleon; processes and inserts into `etf_data.db` for tracking ETF holdings (e.g., relevant to GME shorts via ETFs like XRT).

- **fetch_tickers.py**: Downloads historical stock price data for specified tickers (e.g., GME) using yfinance; stores in `fetcher_data.db` for time-series analysis.

- **ftd_data_pull.py**: Downloads and parses SEC Failure to Deliver (FTD) data in half-month ZIP files; extracts settlement dates, symbols, quantities, and prices, inserting into `ftd_data.db` with deduplication.

- **market_holidays.py**: Fetches and caches US market holidays from Polygon API into `market_holidays.json`; provides a utility function `is_trading_day()` for scheduling pulls.

- **occ_options_pull.py**: Main orchestrator for OCC options data; loads config from `sources.json`, checks trading days, fetches via `OCCSeriesFetcher`, and inserts into `gme_data.db` (or renamed DB) with delta checks to avoid overwrites.

- **database.py**: Provides SQLite utilities including `init_db()` for table creation, `get_last_date()` for incremental pulls, and `insert_data()` for chunked, deduplicated inserts.

- **cron_logger.py**: Logs job executions (status, rows inserted, errors, duration) to `cron_logs.db`; used by pull scripts for monitoring cron jobs.

- **send_daily_summary.py**: Compiles end-of-day summaries from logs/databases and sends via Mailgun email; useful for daily reports on data pulls.

## Usage
- Run pulls via cron (e.g., `0 16 * * 1-5 python scripts/occ_options_pull.py` for market close).
- Set `DROP_TABLE=true` env var for resets.
- Requires API keys (Polygon, Mailgun) in `.env`.
- Config: `config/sources.json` defines fetch params per script.

For full details, see inline docstrings and logs in `data/`.
