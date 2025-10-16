# GMEScraper
GME Data Scraper

# Processing loop
``` mermaid
flowchart TD
    A["Load sources.json\n(e.g., 20 entries)"] --> B["Loop over Sources\n(Sequential, 1-5s each)"]
    B --> C["Instantiate Fetcher\n(e.g., PolygonOptionsFetcher)"]
    C --> G["Check Delta\n(New date > last?)\nIf no: Skip/Log"]
    G -->|Yes| D["Fetch Data\n(API/Scrape/File)\ne.g., Polygon API"]
    G -->|No| I["Log Success/Failure\n(To file/DB/email)"]
    D --> E["Normalize DF\n(Add date, source, timestamp)\ne.g., pd.to_datetime()"]
    E --> H["Upsert to SQLite\n(Append if new)"]
    H --> I
    I --> J["End: Daily Run Complete ~2-3 min"]
```
