import pandas as pd
import glob
import os
import numpy as np

def clean_phone_for_match(phone):
    s = str(phone).strip()
    if s.startswith('+353'): s = '0' + s[4:]
    elif s.startswith('353'): s = '0' + s[3:]
    return s.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')

def debug_analysis():
    print("--- DEBUGGING DATA LOADING ---")
    data_dir = 'data'
    
    # 1. Load Main Logs
    files = glob.glob(os.path.join(data_dir, 'CallLogLastWeek_*.csv'))
    dfs = []
    for f in files:
        print(f"Loading {f}...")
        try:
            # Minimal loading to check dates
            temp = pd.read_csv(f)
            # Check for date column
            date_col = next((c for c in temp.columns if 'start' in c.lower() or 'date' in c.lower()), None)
            if date_col:
                temp[date_col] = pd.to_datetime(temp[date_col], dayfirst=True, errors='coerce')
                print(f"  -> Date Range: {temp[date_col].min()} to {temp[date_col].max()}")
                print(f"  -> Rows: {len(temp)}")
            dfs.append(temp)
        except Exception as e:
            print(f"  -> Error: {e}")

    # 2. Check Combined Main Log
    # We need to replicate the cleaning/loading logic from call_log_analyzer roughly
    # But let's just look at the raw combined first
    if not dfs:
        print("No main logs found.")
        return

    # 3. Check Trade Numbers Extraction
    # We need the processed df to get customer_type. 
    # Let's try to load the 'reports/call_logs_cleaned.csv' if it exists, as that's what the report used
    cleaned_path = 'reports/call_logs_cleaned.csv'
    if os.path.exists(cleaned_path):
        print(f"\n--- ANALYZING CLEANED LOGS ({cleaned_path}) ---")
        df = pd.read_csv(cleaned_path)
        df['call_start'] = pd.to_datetime(df['call_start'])
        
        print(f"Total Rows: {len(df)}")
        print(f"Date Range: {df['call_start'].min()} to {df['call_start'].max()}")
        
        # Check Week Assignment
        max_date = df['call_start'].max()
        print(f"Max Date: {max_date}")
        
        week1_start = max_date - pd.Timedelta(days=7)
        week1_end = max_date
        week2_start = week1_start - pd.Timedelta(days=7)
        week2_end = week1_start
        
        print(f"Week 1: {week1_start} to {week1_end}")
        print(f"Week 2: {week2_start} to {week2_end}")
        
        # Manual check of week 2 data
        week2_data = df[(df['call_start'] > week2_start) & (df['call_start'] <= week2_end)]
        print(f"Actual Week 2 Data Count (Manual Filter): {len(week2_data)}")
        
        if 'week' in df.columns:
            print(f"Week Column Counts: {df['week'].value_counts().to_dict()}")
            
        # Check Trade Numbers
        trade_df = df[df['customer_type'] == 'trade']
        print(f"Trade Rows: {len(trade_df)}")
        if not trade_df.empty:
            sample_trade = trade_df['from_number'].head(5).tolist()
            print(f"Sample Trade Numbers (Raw): {sample_trade}")
            cleaned_trade = [clean_phone_for_match(x) for x in sample_trade]
            print(f"Sample Trade Numbers (Cleaned): {cleaned_trade}")
            
            trade_numbers = set(trade_df['from_number'].apply(clean_phone_for_match).unique())
            print(f"Unique Trade Numbers in Set: {len(trade_numbers)}")
            
            # Check specific number if known
            test_num = '0870000000' # Placeholder
            # You can add a known trade number here if you see one in the report
            
    # 4. Check Abandoned Logs
    print("\n--- ANALYZING ABANDONED LOGS ---")
    abd_path = 'reports/abandoned_logs_cleaned.csv'
    if os.path.exists(abd_path):
        abd_df = pd.read_csv(abd_path)
        print(f"Total Abandoned: {len(abd_df)}")
        if 'Caller ID' in abd_df.columns:
            print(f"Sample Caller IDs: {abd_df['Caller ID'].head(5).tolist()}")
            
            # Test Matching
            if 'trade_numbers' in locals():
                print("Testing Matching Logic...")
                matches = 0
                for idx, row in abd_df.iterrows():
                    caller_id = row['Caller ID']
                    cleaned = clean_phone_for_match(caller_id)
                    if cleaned in trade_numbers:
                        matches += 1
                        if matches <= 5:
                            print(f"  MATCH: {caller_id} -> {cleaned}")
                print(f"Total Matches Found: {matches}")
        else:
            print("Caller ID column missing in abandoned logs")

if __name__ == "__main__":
    debug_analysis()
