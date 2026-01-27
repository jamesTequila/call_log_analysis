import pandas as pd
import glob
import os

# Check the actual date distribution
files = glob.glob(os.path.join('data', 'CallLogLastWeek_*.csv'))

all_data = []
for f in files:
    df = pd.read_csv(f)
    df['Call Time'] = pd.to_datetime(df['Call Time'], errors='coerce')
    df = df[~df['Call Time'].isna()]
    df['source_file'] = os.path.basename(f)
    all_data.append(df)

combined = pd.concat(all_data, ignore_index=True)
combined['date'] = combined['Call Time'].dt.date

print("Date distribution across all files:")
print(combined.groupby('date')['Call ID'].count().sort_index())

print("\n\nDate distribution by file:")
print(combined.groupby(['source_file', 'date'])['Call ID'].count().sort_index())

print(f"\n\nTotal date range: {combined['Call Time'].min()} to {combined['Call Time'].max()}")
print(f"Number of days: {(combined['Call Time'].max() - combined['Call Time'].min()).days + 1}")

# Check if the user wants to split this into 2 weeks
# Perhaps Week 1 = Mon-Wed, Week 2 = Thu-Sat?
combined['day_of_week'] = combined['Call Time'].dt.day_name()
print("\n\nDistribution by day of week:")
print(combined['day_of_week'].value_counts())
