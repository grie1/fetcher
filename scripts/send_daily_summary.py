#!/usr/bin/env python3
import pandas as pd
import sqlite3
import os
import json
import logging  # Add this!
import re  # For text fallback regex
from datetime import datetime, date
import requests
from dotenv import load_dotenv  # pip install python-dotenv

load_dotenv()

# Logging setup (file + console, like other scripts)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/summary.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join('data', 'cron_logs.db')
MAILGUN_DOMAIN = os.getenv('MAILGUN_DOMAIN')
MAILGUN_KEY = os.getenv('MAILGUN_API_KEY')
TO_EMAIL = os.getenv('SUMMARY_EMAIL', 'fetcher@grismer.net')
FROM_EMAIL = f'Daily Pull <no-reply@{MAILGUN_DOMAIN}>'

def get_today_summary():
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('''
    SELECT job_name, status, rows_inserted, errors, duration_seconds, notes, run_timestamp
    FROM job_logs WHERE DATE(run_timestamp) = ?
    ORDER BY run_timestamp DESC
    ''', conn, params=(today,))
    conn.close()
    if df.empty:
        html = "<p>No jobs ran today.</p>"
        status_counts = {}
        return html, status_counts

    style = """
    <style>
    table { width: 100%; font-family: Arial, sans-serif; font-size: 14px; border: 1px solid #ddd; }
    th { padding: 12px; background: #f4f4f4; color: #333; font-weight: bold; border-bottom: 2px solid #ccc; }
    td { padding: 12px; border-bottom: 1px solid #eee; }
    tr:nth-child(even) td { background: #f9f9f9; }
    .status-pill { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; display: inline-block; border: 1px solid; }
    .status-success { background: #d4edda; color: #155724; border-color: #c3e6cb; }
    .status-warning { background: #fff3cd; color: #856404; border-color: #ffeaa7; }
    .status-error { background: #f8d7da; color: #721c24; border-color: #f5c6cb; }
    </style>
    """
    html = (
        "<h2 style='color: #333; font-family: Arial, sans-serif; margin-bottom: 12px;'>Daily Fetcher Summary</h2>"
        f"{style}<table border='1' cellpadding='0' cellspacing='0'>"
        "<tr><th>Time</th><th>Job</th><th>Status</th><th>Rows</th><th>Duration (s)</th><th>Notes / Errors</th></tr>"
    )
    for idx, row in df.iterrows():
        run_time = datetime.fromisoformat(row['run_timestamp']).strftime('%H:%M')
        status_emoji = {'success': '✅', 'warning': '⚠️', 'error': '❌'}[row['status']]
        status_class = f"status-pill status-{row['status']}"
        errors_str = json.loads(row['errors'])[0][:100] + '...' if row['errors'] != '[]' else ''
        notes = row['notes'] or errors_str
        html += (
            "<tr>"
            f"<td>{run_time}</td>"
            f"<td>{row['job_name']}</td>"
            f"<td><span class='{status_class}'>{status_emoji} {row['status']}</span></td>"
            f"<td>{row['rows_inserted']}</td>"
            f"<td>{row['duration_seconds']:.1f}</td>"
            f"<td>{notes}</td>"
            "</tr>"
        )
    html += "</table>"
    html += f"<p style='font-family: Arial, sans-serif; color: #666; font-size: 12px;'>Generated: {datetime.now().isoformat()}</p>"
    return html, df['status'].value_counts().to_dict()  # For subject

def send_email(subject: str, html_body: str):
    if not MAILGUN_DOMAIN or not MAILGUN_KEY:
        logger.warning("Mailgun env vars missing; skipping send.")
        return
    resp = requests.post(
        f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_KEY),
        data={
            "from": FROM_EMAIL,
            "to": [TO_EMAIL],
            "subject": subject,
            "html": html_body,
            "text": re.sub(r'<[^>]*>', '', html_body).replace('&', 'and')  # Proper regex for HTML tags
        }
    )
    if resp.status_code == 200:
        logger.info("Email sent successfully.")
    else:
        error_msg = resp.json() if 'application/json' in resp.headers.get('content-type', '') else resp.text
        logger.error(f"Email failed: {resp.status_code} - {error_msg}")
        # Optional: Log to cron_logs.db as 'warning'
        try:
            from cron_logger import log_job_summary
            log_job_summary('Daily Summary Email', 'warning', 0, [f"Status: {resp.status_code} - {error_msg}"], 0, "Delivery issue - retry tomorrow")
        except ImportError:
            logger.warning("cron_logger not available; skipping DB log.")

if __name__ == '__main__':
    html, status_counts = get_today_summary()
    errors_count = status_counts.get('error', 0)
    subject = f"Fetcher Daily Summary ({date.today()}) {'🚨 ERRORS' if errors_count > 0 else '✅ All Good'}"
    send_email(subject, html)
    logger.info(f"Summary prepared: {len(html)} chars, Status: {status_counts}")
