import csv
import os
from datetime import datetime
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
CSV_PATH = os.path.join(DATA_DIR, 'weekly_data.csv')

# Define CSV columns
COLUMNS = [
    'week_start', 'week_end', 
    'total_calls', 
    'retail_calls', 'trade_calls', 
    'abandoned_total', 
    'retail_abandoned', 'trade_abandoned',
    'report_generated_date'
]

def initialize_db():
    """Initialize the CSV database if it doesn't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)
        print(f"Initialized weekly data CSV at: {CSV_PATH}")

def load_week_data(start_date, end_date):
    """
    Load data for a specific week range.
    Returns a dictionary of metrics if found, else None.
    Dates should be strings in 'DD/MM/YYYY' format or datetime objects.
    """
    if not os.path.exists(CSV_PATH):
        return None

    # Normalize dates to string format 'DD/MM/YYYY' for comparison
    if isinstance(start_date, datetime):
        start_date = start_date.strftime('%d/%m/%Y')
    if isinstance(end_date, datetime):
        end_date = end_date.strftime('%d/%m/%Y')

    try:
        df = pd.read_csv(CSV_PATH)
        # Ensure dates are strings for comparison
        df['week_start'] = df['week_start'].astype(str)
        df['week_end'] = df['week_end'].astype(str)

        match = df[(df['week_start'] == start_date) & (df['week_end'] == end_date)]
        
        if not match.empty:
            # Return the last matching entry (in case of duplicates, though we avoid them)
            row = match.iloc[-1].to_dict()
            return row
        
        return None

    except Exception as e:
        print(f"Error loading week data from CSV: {e}")
        return None

def save_week_data(metrics):
    """
    Save or update metrics for a week.
    metrics dict must contain:
    - start_date, end_date
    - total, retail, trade
    - abandoned, abandoned_retail, abandoned_trade
    """
    initialize_db()
    
    start_date = metrics.get('start_date')
    end_date = metrics.get('end_date')
    
    # Check if entry exists to avoid duplicates (could update instead, but appending is safer for log history?)
    # For this use case, we probably want to OVERWRITE or UPDATE if it exists to fix errors.
    # Let's read all, filter out this week if exists, and append new.
    
    rows = []
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    
    # Remove existing entry for this week
    rows = [r for r in rows if not (r['week_start'] == start_date and r['week_end'] == end_date)]
    
    # Prepare new row
    new_row = {
        'week_start': start_date,
        'week_end': end_date,
        'total_calls': metrics.get('total', 0),
        'retail_calls': metrics.get('retail', 0),
        'trade_calls': metrics.get('trade', 0),
        'abandoned_total': metrics.get('abandoned', 0),
        'retail_abandoned': metrics.get('abandoned_retail', 0),
        'trade_abandoned': metrics.get('abandoned_trade', 0),
        'report_generated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    rows.append(new_row)
    
    # Sort by date (optional but nice)
    # rows.sort(key=lambda x: datetime.strptime(x['week_start'], '%d/%m/%Y') if x['week_start'] else datetime.min)

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Saved weekly data for {start_date} - {end_date} to CSV.")

def get_all_weeks():
    """Return all stored weeks."""
    if not os.path.exists(CSV_PATH):
        return []
    return pd.read_csv(CSV_PATH).to_dict('records')
