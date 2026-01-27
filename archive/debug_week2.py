import pandas as pd
import glob
import os

# Check what the actual data looks like
files = glob.glob(os.path.join('data', 'CallLogLastWeek_*.csv'))
print("Files found:", files)

for f in files:
    print(f"\n{'='*60}")
    print(f"File: {f}")
    df = pd.read_csv(f)
    df['Call Time'] = pd.to_datetime(df['Call Time'], errors='coerce')
    df = df[~df['Call Time'].isna()]
    
    print(f"Date range: {df['Call Time'].min()} to {df['Call Time'].max()}")
    print(f"Total rows: {len(df)}")
    
    # Calculate week like cleaning.py does
    df['week_start'] = df['Call Time'].dt.normalize() - pd.to_timedelta(df['Call Time'].dt.dayofweek, unit='D')
    max_week_start = df['week_start'].max()
    df['week'] = (max_week_start - df['week_start']).dt.days // 7 + 1
    
    print(f"Max week_start: {max_week_start}")
    print(f"Week distribution:")
    print(df['week'].value_counts().sort_index())
    
    # Check unique call IDs
    print(f"Unique Call IDs: {df['Call ID'].nunique()}")

# Now check what happens after aggregation
print("\n" + "="*60)
print("AFTER AGGREGATION (simulating cleaning.py)")
print("="*60)

from cleaning import run_cleaning

all_dfs = []
for f in files:
    cleaned = run_cleaning(f)
    call_level = cleaned.call_level_df
    print(f"\nFile: {f}")
    print(f"Call level rows: {len(call_level)}")
    print(f"Week distribution:")
    print(call_level['week'].value_counts().sort_index())
    all_dfs.append(call_level)

# Combine like analyze_calls does
combined = pd.concat(all_dfs, ignore_index=True)
combined = combined.drop_duplicates(subset=['Call ID'])
print(f"\nCombined (after dedup):")
print(f"Total rows: {len(combined)}")
print(f"Week distribution:")
print(combined['week'].value_counts().sort_index())
