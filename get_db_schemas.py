#!/usr/bin/env python3
import sqlite3
import os

DATA_DIR = "data"   # adjust if needed

for fname in os.listdir(DATA_DIR):
    if not fname.endswith(".db"):
        continue

    db_path = os.path.join(DATA_DIR, fname)
    print(f"\n=== {db_path} ===")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    tables = cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
    """).fetchall()

    for (table,) in tables:
        print(f"\n[Table] {table}")

        # columns + types
        cols = cur.execute(f"PRAGMA table_info({table});").fetchall()
        for col in cols:
            # col tuple: (cid, name, type, notnull, dflt_value, pk)
            print(f"  {col[1]:20s} {col[2]:10s} {'PK' if col[5] else ''}")

        # row count
        try:
            (count,) = cur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()
            print(f"  Rows: {count}")
        except:
            pass
    sample = cur.execute(f"SELECT * FROM {table} LIMIT 5;").fetchall()
    if sample:
        print("  Sample rows:")
        for row in sample:
            print("   ", row)

    conn.close()

