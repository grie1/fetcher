import json
import os
from datetime import date
from polygon import RESTClient
import logging

logger = logging.getLogger(__name__)
client = RESTClient(api_key=os.getenv('POLYGON_API_KEY', 'demo'))  # .env or demo

SINGLE_FILE = 'market_holidays.json'

def load_holidays_dict() -> dict:
    """Load {year_str: list of date_strs}."""
    if os.path.exists(SINGLE_FILE):
        with open(SINGLE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_holidays_dict(holidays_dict: dict):
    """Save sorted dict to JSON."""
    sorted_dict = {k: sorted(v) for k, v in holidays_dict.items()}
    with open(SINGLE_FILE, 'w') as f:
        json.dump(sorted_dict, f, indent=2)
    print(f"Saved {len(sorted_dict)} years to {SINGLE_FILE} ({sum(len(v) for v in sorted_dict.values())} total dates).")
    logger.info(f"Updated holidays for {len(sorted_dict)} years.")

def get_holidays_for_year(year: int) -> list:
    """Pull API, filter/dedup to year."""
    year_str = str(year)
    holidays_dict = load_holidays_dict()
    
    if year_str in holidays_dict:
        print(f"Cache hit for {year}: {len(holidays_dict[year_str])} dates.")
        return holidays_dict[year_str]
    
    print(f"Pulling {year} from API...")
    try:
        year_start = f'{year}-01-01'
        year_end = f'{year}-12-31'
        holidays_resp = client.get_market_holidays(params={'from': year_start, 'to': year_end})
        raw_holidays = [h.date for h in holidays_resp]
        # Filter to year, dedup
        holidays = sorted(list(set(d for d in raw_holidays if d.startswith(year_str))))
        print(f"API pulled {len(raw_holidays)} raw; {len(holidays)} unique for {year}.")
    except Exception as e:
        print(f"API error for {year} ({e}). No holidays available.")
        holidays = []
    
    # Add and save
    holidays_dict[year_str] = holidays
    save_holidays_dict(holidays_dict)
    return holidays

def is_trading_day(today: date = None) -> bool:
    """Mon-Fri and not in available holidays."""
    if today is None:
        today = date.today()
    holidays = get_holidays_for_year(today.year)
    today_str = today.strftime('%Y-%m-%d')
    return today.weekday() < 5 and today_str not in holidays

# Standalone: Pull current + next
if __name__ == "__main__":
    current_year = date.today().year
    print(f"API pull for {current_year} + {current_year + 1}.")
    get_holidays_for_year(current_year)
    get_holidays_for_year(current_year + 1)
    
    print(f"\n{current_year} holidays (available): {get_holidays_for_year(current_year)}")
    print(f"\n{current_year + 1} holidays (available): {get_holidays_for_year(current_year + 1)}")
    print("\nTests:")
    print(f"Today ({date.today()}): Trading? {is_trading_day()}")
    print(f"Late 2025 holiday (2025-12-25): Trading? {is_trading_day(date(2025, 12, 25))}")
