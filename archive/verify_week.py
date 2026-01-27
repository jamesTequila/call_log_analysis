import pandas as pd
from cleaning import run_cleaning
import os

def verify_week_logic():
    data_path = 'data/combined_call_logs.csv'
    if not os.path.exists(data_path):
        print(f"File not found: {data_path}")
        return

    print(f"Processing {data_path}...")
    cleaned_data = run_cleaning(data_path)
    df = cleaned_data.call_level_df
    
    print("Columns:", df.columns)
    
    if 'week' not in df.columns:
        print("Error: 'week' column missing.")
        return

    print("\nSample data (date, week_start, week):")
    print(df[['call_start', 'week_start', 'week']].head(10))
    
    print("\nWeek counts:")
    print(df['week'].value_counts().sort_index())
    
    # Check logic
    max_week = df['week'].min() # Should be 1 if logic is correct? No, max_week_start corresponds to week 1.
    # Logic: (max - current) // 7 + 1.
    # If current == max, result is 1.
    # So min value of week should be 1.
    
    min_week_val = df['week'].min()
    max_week_val = df['week'].max()
    
    print(f"\nMin week value: {min_week_val}")
    print(f"Max week value: {max_week_val}")
    
    if min_week_val == 1:
        print("Verification PASSED: Week numbering starts at 1.")
    else:
        print(f"Verification FAILED: Week numbering starts at {min_week_val}.")

if __name__ == "__main__":
    verify_week_logic()
