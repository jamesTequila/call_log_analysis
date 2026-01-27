import pandas as pd
import os

abd_path = os.path.join('data', 'combined_abandoned_call_logs.csv')
df = pd.read_csv(abd_path)

print(f"Total Rows: {len(df)}")
print(f"Unique Caller ID + Time: {df.duplicated(subset=['Caller ID', 'Call Time']).sum()} duplicates")

# Check overlap between the two source files if possible?
# The combined file doesn't have source filename.
# But we can check if the same call appears multiple times.

# Also check duplicates in Main Log if we were to combine them
files = ['CallLogLastWeek_1711_USfwQxI8xPVeXEu6co83.csv', 'CallLogLastWeek_2411_Xbks975ieCQI7FuSBmwg.csv']
dfs = []
for f in files:
    p = os.path.join('data', f)
    if os.path.exists(p):
        dfs.append(pd.read_csv(p))

if len(dfs) == 2:
    combined_main = pd.concat(dfs)
    print(f"Combined Main Raw Rows: {len(combined_main)}")
    print(f"Main Duplicates (Call ID): {combined_main.duplicated(subset=['Call ID']).sum()}")
