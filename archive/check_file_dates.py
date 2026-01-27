import pandas as pd
import glob
import os

files = glob.glob(os.path.join('data', 'CallLogLastWeek_*.csv'))

for f in files:
    print(f"\n{'='*60}")
    print(f"File: {os.path.basename(f)}")
    df = pd.read_csv(f)
    df['Call Time'] = pd.to_datetime(df['Call Time'], errors='coerce')
    df = df[~df['Call Time'].isna()]
    
    print(f"Date range: {df['Call Time'].min().date()} to {df['Call Time'].max().date()}")
    print(f"Total rows: {len(df)}")
    
    # Group by date
    df['date'] = df['Call Time'].dt.date
    date_counts = df.groupby('date')['Call ID'].count().sort_index()
    print("\nCalls per day:")
    for date, count in date_counts.items():
        print(f"  {date}: {count}")
