import pandas as pd
import os

def load_and_filter(file_path, start_date, end_date):
    print(f"Loading {file_path}...")
    try:
        df = pd.read_csv(file_path)
        # Clean columns
        df.columns = df.columns.str.strip()
        print(f"Columns in {os.path.basename(file_path)}: {list(df.columns)}")
        
        # Standardize column names if needed, but assuming standard format
        if 'Call Time' in df.columns:
            df['call_start'] = pd.to_datetime(df['Call Time'], errors='coerce')
            # Drop rows with invalid dates (e.g. Totals)
            df = df.dropna(subset=['call_start'])
        
        # Filter for range
        mask = (df['call_start'] >= start_date) & (df['call_start'] <= end_date)
        filtered_df = df[mask]
        
        print(f" -> Total Rows: {len(df)}")
        print(f" -> Rows in range ({start_date} - {end_date}): {len(filtered_df)}")
        
        if filtered_df.empty:
             return pd.DataFrame(columns=df.columns)

        # Deduplicate by Call ID to get unique calls
        unique_calls = filtered_df.drop_duplicates(subset=['Call ID'])
        print(f" -> Unique Calls in range: {len(unique_calls)}")
        
        return unique_calls
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

# Paths
file_1201 = os.path.join('data', 'CallLogLastWeek_1201_mWtBCfAskIyaqhZe22c1.csv')
file_1901 = os.path.join('data', 'CallLogLastWeek_1901_46eIDBjWKLIigqcwo0fy.csv')

# Target Week: Jan 5 to Jan 11 (End of day)
start_date = pd.Timestamp('2026-01-05')
end_date = pd.Timestamp('2026-01-11 23:59:59')

print(f"Comparing data for period: {start_date} to {end_date}\n")

df1 = load_and_filter(file_1201, start_date, end_date)
print(f"DF1 ({os.path.basename(file_1201)}) Date Range: {df1['call_start'].min()} to {df1['call_start'].max()}")

df2 = load_and_filter(file_1901, start_date, end_date)
# Load df2 without filter to check its range
df2_full = pd.read_csv(file_1901)
if 'Call Time' in df2_full.columns:
    df2_full['call_start'] = pd.to_datetime(df2_full['Call Time'], errors='coerce')
    df2_full = df2_full.dropna(subset=['call_start'])
    print(f"DF2 ({os.path.basename(file_1901)}) Full Date Range: {df2_full['call_start'].min()} to {df2_full['call_start'].max()}")

# Check deduplication
combined = pd.concat([df1], ignore_index=True).drop_duplicates(subset=['Call ID'])
print(f"combined.shape: {combined.shape}")

print("\n--- Breakdown by Status (Common ID match) ---")
# Count by Status
status_counts = combined['Status'].value_counts()
print(status_counts)

print(f"\nSum 'Answered': {status_counts.get('Answered', 0)}")
print(f"Sum 'Unanswered': {status_counts.get('Unanswered', 0)}")
print(f"Total Unique: {len(combined)}")

# Hypothesis: Old Report (1788) = Answered + Abandoned?
# Or Retail/Trade only counts Answered?
# Abandoned (from separate file) = 269.
# If Retail+Trade = 1519.
# 1519 seems close to Answered count?

# Let's check 'customer_type' logic if possible.
# But just Status breakdown is a good first step.


