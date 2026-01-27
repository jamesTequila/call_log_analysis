import pandas as pd
import glob
import os

data_dir = 'data'
files = glob.glob(os.path.join(data_dir, 'CallLogLastWeek_*.csv'))

print("=" * 80)
print("CHECKING ALL CALL LOG FILES")
print("=" * 80)

for f in files:
    df = pd.read_csv(f)
    df['Call Time'] = pd.to_datetime(df['Call Time'], errors='coerce')
    
    max_date = df['Call Time'].max()
    min_date = df['Call Time'].min()
    
    print(f"\n{os.path.basename(f)}:")
    print(f"  Min date: {min_date}")
    print(f"  Max date: {max_date}")
    print(f"  Total rows: {len(df)}")
    
    # Check for December dates
    dec_dates = df[df['Call Time'].dt.month == 12]
    if len(dec_dates) > 0:
        print(f"  ⚠️  WARNING: Contains {len(dec_dates)} December dates!")
        print(f"      First Dec date: {dec_dates['Call Time'].min()}")

print("\n" + "=" * 80)

# Also check abandoned files
abd_files = glob.glob(os.path.join(data_dir, 'AbandonedCalls*.csv'))
if abd_files:
    print("\nCHECKING ABANDONED CALL FILES")
    print("=" * 80)
    
    for f in abd_files:
        df = pd.read_csv(f)
        df['Call Time'] = pd.to_datetime(df['Call Time'], errors='coerce')
        
        max_date = df['Call Time'].max()
        min_date = df['Call Time'].min()
        
        print(f"\n{os.path.basename(f)}:")
        print(f"  Min date: {min_date}")
        print(f"  Max date: {max_date}")
        print(f"  Total rows: {len(df)}")
        
        # Check for December dates
        dec_dates = df[df['Call Time'].dt.month == 12]
        if len(dec_dates) > 0:
            print(f"  ⚠️  WARNING: Contains {len(dec_dates)} December dates!")
            print(f"      First Dec date: {dec_dates['Call Time'].min()}")
