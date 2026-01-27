import pandas as pd
import glob
import os

def verify_integrity():
    print("=== DATA INTEGRITY CHECK ===")
    
    # 1. Check Raw Data
    raw_files = glob.glob(os.path.join('data', 'CallLogLastWeek_*.csv'))
    raw_count = 0
    for f in raw_files:
        try:
            df = pd.read_csv(f)
            raw_count += len(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    print(f"Raw Files Found: {len(raw_files)}")
    print(f"Total Raw Rows: {raw_count}")
    
    # 2. Check Processed Data
    cleaned_path = 'reports/call_logs_cleaned.csv'
    if not os.path.exists(cleaned_path):
        print("ERROR: Cleaned log not found!")
        return
        
    cleaned_df = pd.read_csv(cleaned_path)
    cleaned_count = len(cleaned_df)
    print(f"Processed Rows: {cleaned_count}")
    
    # 3. Check for Duplicates
    if 'Call ID' in cleaned_df.columns:
        dupes = cleaned_df.duplicated(subset=['Call ID']).sum()
        print(f"Duplicate Call IDs in Processed: {dupes}")
        if dupes > 0:
            print("  WARNING: Duplicates found!")
    else:
        print("WARNING: 'Call ID' column missing.")
        
    # 4. Check Date Ranges
    if 'call_start' in cleaned_df.columns:
        cleaned_df['call_start'] = pd.to_datetime(cleaned_df['call_start'])
        min_date = cleaned_df['call_start'].min()
        max_date = cleaned_df['call_start'].max()
        print(f"Date Range: {min_date} to {max_date}")
    
    # 5. Check Abandoned Data
    abd_path = 'reports/abandoned_logs_cleaned.csv'
    if os.path.exists(abd_path):
        abd_df = pd.read_csv(abd_path)
        print(f"Abandoned Rows: {len(abd_df)}")
        
        # Check for overlap with main log (should be none usually, or handled)
        # Actually, abandoned calls are usually NOT in the main answered log, or are separate.
        # Let's just check if any Call IDs overlap if they exist
        if 'Call ID' in abd_df.columns and 'Call ID' in cleaned_df.columns:
             overlap = len(set(cleaned_df['Call ID']).intersection(set(abd_df['Call ID'])))
             print(f"Overlap between Main and Abandoned IDs: {overlap}")
    else:
        print("Abandoned log not found.")

if __name__ == "__main__":
    verify_integrity()
