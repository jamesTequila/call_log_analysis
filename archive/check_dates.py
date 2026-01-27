import pandas as pd
import os

# Load Abandoned
abd_path = os.path.join('data', 'combined_abandoned_call_logs.csv')
abd = pd.read_csv(abd_path)
abd['Call Time'] = pd.to_datetime(abd['Call Time'], errors='coerce')
print("Abandoned Logs Date Range:")
print(f"Min: {abd['Call Time'].min()}")
print(f"Max: {abd['Call Time'].max()}")
print("Counts by Week Start (Monday):")
abd['week_start'] = abd['Call Time'].dt.normalize() - pd.to_timedelta(abd['Call Time'].dt.dayofweek, unit='D')
print(abd['week_start'].value_counts().sort_index())

print("-" * 30)

# Load Main (Raw)
main_path = os.path.join('data', 'CallLogLastWeek_2411_Xbks975ieCQI7FuSBmwg.csv')
main = pd.read_csv(main_path)
main['Call Time'] = pd.to_datetime(main['Call Time'], errors='coerce')
print("Main Log Date Range:")
print(f"Min: {main['Call Time'].min()}")
print(f"Max: {main['Call Time'].max()}")
print("Counts by Week Start (Monday):")
main['week_start'] = main['Call Time'].dt.normalize() - pd.to_timedelta(main['Call Time'].dt.dayofweek, unit='D')
print(main['week_start'].value_counts().sort_index())
