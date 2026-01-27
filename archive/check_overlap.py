import pandas as pd
import os

# Load main log
main_path = os.path.join('data', 'CallLogLastWeek_2411_Xbks975ieCQI7FuSBmwg.csv')
main_df = pd.read_csv(main_path)
print(f"Main Log Rows: {len(main_df)}")
print("Main Columns:", main_df.columns.tolist())

# Load abandoned log
abd_path = os.path.join('data', 'combined_abandoned_call_logs.csv')
abd_df = pd.read_csv(abd_path)
print(f"Abandoned Log Rows: {len(abd_df)}")
print("Abandoned Columns:", abd_df.columns.tolist())

# Check for Call ID in abandoned
if 'Call ID' in abd_df.columns:
    common_ids = set(main_df['Call ID']).intersection(set(abd_df['Call ID']))
    print(f"Common Call IDs: {len(common_ids)}")
else:
    print("No Call ID in abandoned logs.")
    # Try matching by Caller ID and Time?
    # Main: 'From', 'Call Time'
    # Abd: 'Caller ID', 'Call Time'
    
    # Normalize numbers
    main_df['From_clean'] = main_df['From'].astype(str).str.replace('.0', '', regex=False)
    abd_df['Caller_clean'] = abd_df['Caller ID'].astype(str).str.replace('.0', '', regex=False)
    
    # Create a key
    main_df['key'] = main_df['Call Time'].astype(str) + "_" + main_df['From_clean']
    abd_df['key'] = abd_df['Call Time'].astype(str) + "_" + abd_df['Caller_clean']
    
    common_keys = set(main_df['key']).intersection(set(abd_df['key']))
    print(f"Common Keys (Time + Number): {len(common_keys)}")
    
    # Check if any abandoned calls are in main log
    print("Sample Common Keys:", list(common_keys)[:5])
