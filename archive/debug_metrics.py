import pandas as pd
import os
import glob

# Check Main Logs
print("=" * 50)
print("MAIN LOGS ANALYSIS")
print("=" * 50)

files = glob.glob(os.path.join('data', 'CallLogLastWeek_*.csv'))
for f in files:
    print(f"\nFile: {f}")
    df = pd.read_csv(f)
    df['Call Time'] = pd.to_datetime(df['Call Time'], errors='coerce')
    df = df[~df['Call Time'].isna()]
    
    # Calculate week
    df['week_start'] = df['Call Time'].dt.normalize() - pd.to_timedelta(df['Call Time'].dt.dayofweek, unit='D')
    max_week_start = df['week_start'].max()
    df['week'] = (max_week_start - df['week_start']).dt.days // 7 + 1
    
    print(f"Date Range: {df['Call Time'].min()} to {df['Call Time'].max()}")
    print(f"Week Distribution:")
    print(df['week'].value_counts().sort_index())
    print(f"Unique Call IDs: {df['Call ID'].nunique()}")

# Check Abandoned Logs
print("\n" + "=" * 50)
print("ABANDONED LOGS ANALYSIS")
print("=" * 50)

abd_path = os.path.join('data', 'combined_abandoned_call_logs.csv')
if os.path.exists(abd_path):
    abd = pd.read_csv(abd_path)
    print(f"\nTotal Rows: {len(abd)}")
    print(f"Columns: {list(abd.columns)}")
    
    # Check for duplicates
    if 'Caller ID' in abd.columns and 'Call Time' in abd.columns:
        dupes = abd.duplicated(subset=['Caller ID', 'Call Time']).sum()
        print(f"Duplicates (Caller ID + Time): {dupes}")
        
        # Deduplicate
        abd_dedup = abd.drop_duplicates(subset=['Caller ID', 'Call Time'])
        print(f"After Dedup: {len(abd_dedup)}")
        
        # Check date range
        abd_dedup['Call Time'] = pd.to_datetime(abd_dedup['Call Time'], errors='coerce')
        print(f"Date Range: {abd_dedup['Call Time'].min()} to {abd_dedup['Call Time'].max()}")
        
        # Calculate week
        abd_dedup['week_start'] = abd_dedup['Call Time'].dt.normalize() - pd.to_timedelta(abd_dedup['Call Time'].dt.dayofweek, unit='D')
        max_week_start = abd_dedup['week_start'].max()
        abd_dedup['week'] = (max_week_start - abd_dedup['week_start']).dt.days // 7 + 1
        
        print(f"Week Distribution:")
        print(abd_dedup['week'].value_counts().sort_index())
        
        # Check customer type if exists
        if 'customer_type' in abd_dedup.columns:
            print(f"\nCustomer Type Distribution:")
            print(abd_dedup['customer_type'].value_counts())
