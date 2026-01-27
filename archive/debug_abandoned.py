import pandas as pd
import os

from cleaning import run_cleaning

def debug_abandoned_analysis():
    print("--- Debugging Abandoned Calls Analysis ---")
    
    # Load Main Call Log using cleaning pipeline
    if os.path.exists('data/combined_call_logs.csv'):
        print("Running cleaning pipeline on main logs...")
        cleaned_data = run_cleaning('data/combined_call_logs.csv')
        main_df = cleaned_data.call_level_df
        print(f"Main Log Loaded (Cleaned): {len(main_df)} rows")
        
        # Check Date Range of Main Log
        # main_df should have 'call_start' now
        print(f"Main Log Date Range: {main_df['call_start'].min()} to {main_df['call_start'].max()}")
        
        # Check Trade Numbers
        trade_df = main_df[main_df['customer_type'] == 'trade']
        trade_numbers = set(trade_df['from_number'].unique())
        print(f"Found {len(trade_numbers)} unique Trade numbers in Main Log")
        print(f"Sample Trade Numbers: {list(trade_numbers)[:5]}")
    else:
        print("ERROR: data/combined_call_logs.csv not found")
        return

    # Load Combined Abandoned Log
    if os.path.exists('data/combined_abandoned_call_logs.csv'):
        abd_df = pd.read_csv('data/combined_abandoned_call_logs.csv')
        print(f"\nAbandoned Log Loaded: {len(abd_df)} rows")
        
        # Check Date Range
        abd_df['Call Time'] = pd.to_datetime(abd_df['Call Time'], errors='coerce')
        print(f"Abandoned Log Date Range: {abd_df['Call Time'].min()} to {abd_df['Call Time'].max()}")
        
        # Calculate Weeks (replicating logic)
        max_date = abd_df['Call Time'].max()
        abd_df['week_start'] = abd_df['Call Time'].dt.normalize() - pd.to_timedelta(abd_df['Call Time'].dt.dayofweek, unit='D')
        max_week_start = abd_df['week_start'].max()
        abd_df['week'] = (max_week_start - abd_df['week_start']).dt.days // 7 + 1
        
        # Week 1 Stats
        week1 = abd_df[abd_df['week'] == 1]
        print(f"\nWeek 1 Abandoned Calls: {len(week1)}")
        if len(week1) > 0:
            print(f"Week 1 Date Range: {week1['Call Time'].min()} to {week1['Call Time'].max()}")
            
            # Check Mapping for Week 1
            week1['is_trade'] = week1['Caller ID'].apply(lambda x: str(x) in trade_numbers)
            trade_count = week1['is_trade'].sum()
            print(f"Week 1 Trade Matches: {trade_count}")
            print(f"Week 1 Retail (Default): {len(week1) - trade_count}")
            
            # Inspect a few Week 1 Caller IDs
            print(f"Sample Week 1 Caller IDs: {week1['Caller ID'].head().tolist()}")
            
            # Check if these IDs exist in Main Log at all
            sample_id = week1['Caller ID'].iloc[0]
            in_main = str(sample_id) in main_df['from_number'].astype(str).values
            print(f"Is sample ID {sample_id} in Main Log? {in_main}")
            if in_main:
                cust_type = main_df[main_df['from_number'].astype(str) == str(sample_id)]['customer_type'].iloc[0]
                print(f"  -> Customer Type in Main Log: {cust_type}")
        else:
            print("WARNING: No calls found for Week 1")

        # Week 2 Stats
        # week2 = abd_df[abd_df['week'] == 2]
        # print(f"\nWeek 2 Abandoned Calls: {len(week2)}")
        # if len(week2) > 0:
        #     week2['is_trade'] = week2['Caller ID'].apply(lambda x: str(x) in trade_numbers)
        #     print(f"Week 2 Trade Matches: {week2['is_trade'].sum()}")

if __name__ == "__main__":
    debug_abandoned_analysis()
