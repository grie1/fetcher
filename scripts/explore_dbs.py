import sqlite3
import pandas as pd
import os
import glob
from typing import Dict, Any

# Set your data directory path (update if needed)
data_dir = '~/Desktop/github/fetcher/data'  # Or use '.' for current dir
data_dir = os.path.expanduser(data_dir)  # Expands ~ to full path

# Find all .db files, skip cron_logs.db
db_files = [f for f in glob.glob(os.path.join(data_dir, '*.db')) 
            if os.path.basename(f) != 'cron_logs.db']

print(f"Found DBs: {[os.path.basename(f) for f in db_files]}")

# Dict to hold all data: {db_name: {table_name: df}}
all_data: Dict[str, Dict[str, pd.DataFrame]] = {}

for db_path in db_files:
    db_name = os.path.basename(db_path)
    print(f"\n--- Processing {db_name} ---")
    
    conn = sqlite3.connect(db_path)
    
    # Get list of tables
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)['name'].tolist()
    print(f"Tables: {tables}")
    
    db_dfs = {}
    for table in tables:
        try:
            # Load full table into DF (add LIMIT if tables are huge, e.g., "SELECT * FROM {} LIMIT 10000".format(table))
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)
            db_dfs[table] = df
            print(f"Loaded {table}: {len(df)} rows, {len(df.columns)} cols")
            print(df.head(2))  # Quick preview
        except Exception as e:
            print(f"Error loading {table}: {e}")
    
    all_data[db_name] = db_dfs
    conn.close()

# Now all_data has everything! Example access:
# df = all_data['gme_data.db']['some_table']
# Or merge across DBs as needed, e.g., pd.concat([all_data['db1']['table'], all_data['db2']['table']])

print("\nAll data loaded into 'all_data' dict. Ready for Monte Carlo sims!")
