import pandas as pd
import os
import sys

# Add current dir to path to import local modules
sys.path.append(os.getcwd())

from cleaning import run_cleaning
from call_log_analyzer import analyze_calls

# Path to 1201 file (Jan 5-11 data)
file_path = os.path.join('data', 'CallLogLastWeek_1201_mWtBCfAskIyaqhZe22c1.csv')

print(f"Analyzing {file_path} using current 'cleaning.py' logic...")

try:
    # Run cleaning (mimics ingest)
    cleaned_data = run_cleaning(file_path)
    df = cleaned_data.call_level_df
    
    # Filter for Jan 5 - Jan 11
    # Note: cleaning.py adds 'call_start'
    start_date = pd.Timestamp('2026-01-05')
    end_date = pd.Timestamp('2026-01-11 23:59:59')
    
    mask = (df['call_start'] >= start_date) & (df['call_start'] <= end_date)
    df_week = df[mask]
    
    print(f"Total Rows in Range: {len(df_week)}")
    
    # Breakdown by Customer Type
    if 'customer_type' in df_week.columns:
        counts = df_week['customer_type'].value_counts()
        print("\nCustomer Type Breakdown (Current Code):")
        print(counts)
        
        retail = counts.get('retail', 0)
        trade = counts.get('trade', 0)
        total_main = retail + trade
        print(f"\nSum (Retail + Trade) = {total_main}")
        
        # Compare to Report numbers
        print(f"Old Report ID (Retail+Trade approx): 1519")
        print(f"New Report ID (Retail+Trade approx): 2100 (derived from 2369 - 269 abd)")
        
    else:
        print("Error: 'customer_type' column missing after cleaning.")

except Exception as e:
    print(f"Error: {e}")
