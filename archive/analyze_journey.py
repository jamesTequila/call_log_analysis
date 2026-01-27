import pandas as pd
import os
import sys

# Redirect stdout to a file with UTF-8 encoding
sys.stdout = open('journey_analysis.txt', 'w', encoding='utf-8')

# Load main log
file_path = os.path.join('data', 'CallLogLastWeek_2411_Xbks975ieCQI7FuSBmwg.csv')
df = pd.read_csv(file_path)

# Fill NA
df['Call Activity Details'] = df['Call Activity Details'].fillna('')

# Group by Call ID and aggregate details
# We want to see if details differ across rows or if one row contains the full story
journey_df = df.groupby('Call ID').agg({
    'Call Activity Details': lambda x: ' | '.join(sorted(set(x))),
    'Direction': 'first',
    'Status': lambda x: ','.join(sorted(set(x.dropna()))),
    'Call Time': 'min'
}).reset_index()

print(f"Total Unique Calls: {len(journey_df)}")
print("-" * 30)

# 1. Analyze "Out of Office" / "Out of hours"
ooo_calls = journey_df[journey_df['Call Activity Details'].str.contains('Out of office', case=False)]
print(f"Calls marked 'Out of office': {len(ooo_calls)}")
if not ooo_calls.empty:
    print("Sample OOO Journey:")
    print(ooo_calls.iloc[0]['Call Activity Details'])

print("-" * 30)

# 2. Analyze Termination
# "Ended by ..."
# Common patterns: "Ended by Voice Agent", "Ended by [Number]", "Ended by [Name]"
def extract_terminator(details):
    if 'Ended by Voice Agent' in details:
        return 'System (Voice Agent)'
    if 'Ended by' in details:
        # Try to extract who ended it
        # e.g. "Ended by 085..." or "Ended by Mark..."
        import re
        m = re.search(r'Ended by ([^:]+)', details)
        if m:
            return m.group(1).strip()
    return 'Unknown'

journey_df['Terminator'] = journey_df['Call Activity Details'].apply(extract_terminator)
print("Termination Stats:")
print(journey_df['Terminator'].value_counts().head(10))

print("-" * 30)

# 3. Analyze Queue presence
queue_calls = journey_df[journey_df['Call Activity Details'].str.contains('Queue', case=False)]
print(f"Calls involving Queue: {len(queue_calls)}")

print("-" * 30)

# 4. Analyze "Voice Agent" (Voicemail/System)
va_calls = journey_df[journey_df['Call Activity Details'].str.contains('Voice Agent', case=False)]
print(f"Calls involving Voice Agent: {len(va_calls)}")

print("-" * 30)

# 5. Check for multi-row journeys that add info
# Find calls where the aggregated detail string is long or contains '|' indicating multiple distinct details
multi_detail_calls = journey_df[journey_df['Call Activity Details'].str.contains(' \| ')]
print(f"Calls with multiple distinct activity rows: {len(multi_detail_calls)}")
if not multi_detail_calls.empty:
    print("Sample Multi-Row Journey:")
    print(multi_detail_calls.iloc[0]['Call Activity Details'])
