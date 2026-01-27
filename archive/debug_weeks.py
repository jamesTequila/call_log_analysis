
import pandas as pd
import glob
import os
from call_analytics_utils import add_week_label

def check_weeks():
    data_dir = 'data'
    files = glob.glob(os.path.join(data_dir, 'CallLogLastWeek_*.csv'))
    dfs = []
    for f in files:
        dfs.append(pd.read_csv(f))
    
    if not dfs:
        print("No files found")
        return

    df = pd.concat(dfs, ignore_index=True)
    df['Call Time'] = pd.to_datetime(df['Call Time'], dayfirst=True, errors='coerce')
    max_date = df['Call Time'].max()
    print(f"Max Date in dataset: {max_date}")
    print(f"Day of week: {max_date.day_name()}")

    # Test logic
    days_since_monday = max_date.weekday()
    most_recent_monday = max_date - pd.Timedelta(days=days_since_monday)
    
    if days_since_monday == 0:
        week1_start = most_recent_monday - pd.Timedelta(days=7)
        week1_end = most_recent_monday
    else:
        week1_start = most_recent_monday
        week1_end = most_recent_monday + pd.Timedelta(days=7)

    print(f"Week 1 Start: {week1_start}")
    print(f"Week 1 End: {week1_end}")
    
    # Check specific dates
    test_dates = ['2025-11-24 10:00:00', '2025-11-30 10:00:00', '2025-12-01 10:00:00']
    for d in test_dates:
        dt = pd.to_datetime(d)
        is_week1 = (dt >= week1_start) & (dt < week1_end)
        print(f"Date {d} is Week 1? {is_week1}")

if __name__ == "__main__":
    check_weeks()
