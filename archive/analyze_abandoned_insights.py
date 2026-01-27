import pandas as pd
import os
import sys

# Redirect stdout to a file with UTF-8 encoding
sys.stdout = open('abandoned_insights.txt', 'w', encoding='utf-8')

file_path = os.path.join('data', 'combined_abandoned_call_logs.csv')
df = pd.read_csv(file_path)

# Clean column names
df.columns = df.columns.str.strip()
print("Columns:", df.columns.tolist())

print(f"Total Abandoned Calls: {len(df)}")
print("-" * 30)

# 1. Queue Analysis
if 'Queue' in df.columns:
    print("Abandonment by Queue:")
    print(df['Queue'].value_counts())
print("-" * 30)

# 2. Position Analysis
if 'Position' in df.columns:
    print("Abandonment by Queue Position:")
    print(df['Position'].value_counts().sort_index())
    print(f"Average Position: {df['Position'].mean():.1f}")
print("-" * 30)

# 3. Polling Attempts Analysis
if 'Polling Attempts' in df.columns:
    print("Abandonment by Polling Attempts:")
    print(df['Polling Attempts'].value_counts().sort_index())
    print(f"Average Polling Attempts: {df['Polling Attempts'].mean():.1f}")
print("-" * 30)

# 4. Agent State Analysis
if 'Agent State' in df.columns:
    print("Agent State at Abandonment:")
    print(df['Agent State'].value_counts())
print("-" * 30)

# 5. Waiting Time Analysis
# Need to parse "00:00:15" to seconds
def parse_wait(x):
    try:
        if pd.isna(x): return 0
        parts = str(x).split(':')
        if len(parts) == 3: return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
        return 0
    except: return 0

df['wait_sec'] = df['Waiting Time'].apply(parse_wait)
print("Waiting Time Stats (Seconds):")
print(df['wait_sec'].describe())

# Correlation?
# Do people wait longer if they are in a better position?
print("\nAvg Wait Time by Position:")
print(df.groupby('Position')['wait_sec'].mean().sort_index().head(10))
