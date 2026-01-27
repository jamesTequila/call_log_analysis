import pandas as pd
import os

# Load one of the raw call logs
file_path = os.path.join('data', 'CallLogLastWeek_2411_Xbks975ieCQI7FuSBmwg.csv')
df = pd.read_csv(file_path)

import sys

# Redirect stdout to a file with UTF-8 encoding
sys.stdout = open('analysis_output.txt', 'w', encoding='utf-8')

print("Columns:", df.columns.tolist())
print("-" * 20)

# Check unique values for key columns
print("Unique Statuses:", df['Status'].unique())
print("Unique Directions:", df['Direction'].unique())
# Call Activity might be 'Call Activity' or 'Call Activity Details'
if 'Call Activity' in df.columns:
    print("Unique Call Activity (Sample):", df['Call Activity'].unique()[:10])
if 'Call Activity Details' in df.columns:
    print("Unique Call Activity Details (Sample):", df['Call Activity Details'].unique()[:10])

print("-" * 20)

# Check for potential abandoned calls
# Hypothesis: Inbound, Unanswered, or Answered with 0 talking time?
# Let's look at a sample of 'Unanswered' calls
unanswered = df[df['Status'] == 'Unanswered']
if not unanswered.empty:
    print("Sample Unanswered Call:")
    print(unanswered.iloc[0][['Call ID', 'Direction', 'Status', 'Ringing', 'Talking', 'Call Activity Details' if 'Call Activity Details' in df.columns else 'Call Activity']])

print("-" * 20)

# Check if there are calls with Status='Answered' but Talking='00:00:00'
answered_no_talk = df[(df['Status'] == 'Answered') & (df['Talking'] == '00:00:00')]
print(f"Answered calls with 0 talking time: {len(answered_no_talk)}")
if not answered_no_talk.empty:
    print("Sample Answered No Talk:")
    print(answered_no_talk.iloc[0][['Call ID', 'Direction', 'Status', 'Ringing', 'Talking']])

print("-" * 20)

# Group by Call ID to see if a call has multiple rows with different statuses
call_counts = df.groupby('Call ID')['Status'].nunique()
mixed_status_calls = call_counts[call_counts > 1]
print(f"Calls with multiple statuses: {len(mixed_status_calls)}")
if not mixed_status_calls.empty:
    call_id = mixed_status_calls.index[0]
    print(f"Sample Mixed Status Call ({call_id}):")
    print(df[df['Call ID'] == call_id][['Call ID', 'Direction', 'Status', 'Ringing', 'Talking']])
