import pandas as pd
import glob
import os
from cleaning import run_cleaning

# Test the cleaning process
data_dir = 'data'
files = glob.glob(os.path.join(data_dir, 'CallLogLastWeek_*.csv'))

print("=" * 80)
print("TESTING CLEANING PROCESS")
print("=" * 80)

all_dfs = []
for f in files:
    print(f"\nProcessing: {os.path.basename(f)}")
    
    # Clean the file
    cleaned = run_cleaning(f)
    call_df = cleaned.call_level_df
    
    max_date = call_df['call_start'].max()
    min_date = call_df['call_start'].min()
    
    print(f"  After cleaning:")
    print(f"    Min date: {min_date}")
    print(f"    Max date: {max_date}")
    print(f"    Rows: {len(call_df)}")
    
    # Check for December
    dec_rows = call_df[call_df['call_start'].dt.month == 12]
    if len(dec_rows) > 0:
        print(f"    ⚠️  WARNING: {len(dec_rows)} December dates after cleaning!")
        print(f"        Sample dates: {dec_rows['call_start'].head().tolist()}")
    
    all_dfs.append(call_df)

# Combine
print("\n" + "=" * 80)
print("COMBINING ALL FILES")
print("=" * 80)

combined = pd.concat(all_dfs, ignore_index=True)
print(f"\nCombined dataframe:")
print(f"  Min date: {combined['call_start'].min()}")
print(f"  Max date: {combined['call_start'].max()}")
print(f"  Total rows: {len(combined)}")

# Check for December in combined
dec_rows = combined[combined['call_start'].dt.month == 12]
if len(dec_rows) > 0:
    print(f"\n⚠️  WARNING: {len(dec_rows)} December dates in combined data!")
    print(f"Sample December dates:")
    print(dec_rows[['call_start', 'from_number']].head(10))
else:
    print("\n✅ No December dates found!")

# Check week assignments
print(f"\nWeek distribution:")
print(combined['week'].value_counts().sort_index())
