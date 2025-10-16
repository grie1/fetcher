# GMEScraper
GME Data Scraper

# Processing loop
``` mermaid
flowchart TD
    A[Load sources.json<br/>(e.g., 20 entries)] --> B[Loop over Sources<br/>(Sequential, 1-5s each)]
    B --> C[Instantiate Fetcher<br/>(e.g., PolygonOptionsFetcher)]
    C --> G[Check Delta<br/>(New date > last?)<br/>If no: Skip/Log]
    G -->|Yes| D[Fetch Data<br/>(API/Scrape/File)<br/>e.g., Polygon API]
    G -->|No| I[Log Success/Failure<br/>(To file/DB/email)]
    D --> E[Normalize DF<br/>(Add date, source, timestamp)<br/>e.g., pd.to_datetime()]
    E --> H[Upsert to SQLite<br/>(Append if new)]
    H --> I
    I --> J[End: Daily Run Complete ~2-3 min]
```
