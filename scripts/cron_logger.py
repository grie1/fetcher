import sqlite3
import os
import json
from datetime import datetime
from typing import Dict, Any

DB_PATH = os.path.join('data', 'cron_logs.db')

def init_cron_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS job_logs (
        run_timestamp TEXT NOT NULL,
        job_name TEXT NOT NULL,
        status TEXT NOT NULL,  -- 'success', 'warning', 'error'
        rows_inserted INTEGER DEFAULT 0,
        errors TEXT,           -- JSON str of error list
        duration_seconds REAL,
        notes TEXT             -- Freeform, e.g., "Skipped holiday"
    )
    ''')
    conn.commit()
    conn.close()

def log_job_summary(job_name: str, status: str, rows_inserted: int = 0,
                    errors: list = None, duration: float = 0.0, notes: str = ""):
    init_cron_db()  # Idempotent
    errors_json = json.dumps(errors or [])
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO job_logs (run_timestamp, job_name, status, rows_inserted, errors, duration_seconds, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (datetime.now().isoformat(), job_name, status, rows_inserted, errors_json, duration, notes))
    conn.commit()
    conn.close()
