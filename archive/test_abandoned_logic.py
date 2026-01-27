import pandas as pd
import os
from cleaning import run_cleaning

import sys

# Redirect stdout to a file with UTF-8 encoding
sys.stdout = open('test_output.txt', 'w', encoding='utf-8')

# 1. Load and process main logs using current cleaning (but we'll inspect the df before final flags)
# We need to manually replicate the aggregation to check 'statuses' which isn't in the final output of aggregate_to_call_level usually?
# Wait, aggregate_to_call_level returns 'statuses'.

# Let's use run_cleaning on the main file
main_file = os.path.join('data', 'CallLogLastWeek_2411_Xbks975ieCQI7FuSBmwg.csv')
print(f"Processing {main_file}...")
cleaned = run_cleaning(main_file)
df = cleaned.call_level_df

# 2. Apply New Logic
# Filter for Inbound is already done in clean_call_log
# Logic: Talking = 0 AND Statuses does not contain "Answered"
# Note: statuses is a comma-joined string
df['new_is_abandoned'] = (df['talking_total_sec'] == 0) & (~df['statuses'].str.contains('Answered'))

new_abandoned_count = df['new_is_abandoned'].sum()
print(f"New Logic Abandoned Count: {new_abandoned_count}")

# 3. Compare with separate abandoned files
abandoned_file = os.path.join('data', 'combined_abandoned_call_logs.csv')
if os.path.exists(abandoned_file):
    abd_df = pd.read_csv(abandoned_file)
    # Filter for same week/period if possible?
    # The main file is "LastWeek_2411". The combined might have more.
    # Let's just print the total in combined for reference.
    print(f"Total in combined_abandoned_call_logs.csv: {len(abd_df)}")
else:
    print("combined_abandoned_call_logs.csv not found.")

# 4. Inspect a few "New Abandoned" calls
print("\nSample New Abandoned Calls:")
print(df[df['new_is_abandoned']].head()[['call_start', 'from_number', 'statuses', 'talking_total_sec', 'ringing_total_sec']])

# 5. Check for "Answered" calls with 0 talking (Edge case)
answered_zero_talk = df[(df['statuses'].str.contains('Answered')) & (df['talking_total_sec'] == 0)]
print(f"\nInbound Answered calls with 0 talking: {len(answered_zero_talk)}")
if not answered_zero_talk.empty:
    print(answered_zero_talk.head()[['call_start', 'from_number', 'statuses', 'talking_total_sec']])
