# GMEScraper
GME Data Scraper

# ASCII Flowchart
+-------------------+     +-------------------+     +---------------------+
| Load sources.json | --> | Loop over Sources | --> | Instantiate Fetcher |
| (e.g., 20 entries)|     | (Sequential, 1-5s |     | (e.g., PolygonOptionsFetcher) |
|                   |     |  each)            |     |                     |
+-------------------+     +-------------------+     +---------------------+
          |                           |                           |
          v                           v                           v
+-------------------+     +-----------------------------+     +-------------------+
| Fetch Data        | <-- | Normalize DF                | <-- | Check Delta       |
| (API/Scrape/File) |     | (Add date, source, timestamp)|     | (New date > last?)|
| e.g., Polygon API |     | e.g., pd.to_datetime()      |     | If no: Skip/Log   |
+-------------------+     +-----------------------------+     +-------------------+
          |                           |                           |
          +---------------------------+---------------------------+
                              | 
                              v
                    +-------------------+
                    | Upsert to SQLite  |
                    | (Append if new)   |
                    +-------------------+
                              |
                              v
                    +-------------------+
                    | Log Success/Failure|
                    | (To file/DB/email)|
                    +-------------------+

End: Daily Run Complete ~2-3 min
