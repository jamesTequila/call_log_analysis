import pandas as pd
import glob
import os
from cleaning import run_cleaning

# Load and process main logs
files = glob.glob(os.path.join('data', 'CallLogLastWeek_*.csv'))
dfs = []
for f in files:
    cleaned = run_cleaning(f)
    dfs.append(cleaned.call_level_df)

df = pd.concat(dfs, ignore_index=True)
df = df.drop_duplicates(subset=['Call ID'])

print("MAIN LOG ANALYSIS")
print("=" * 60)
print(f"Total calls: {len(df)}")
print(f"\nCustomer type distribution:")
print(df['customer_type'].value_counts())

print(f"\nWeek distribution:")
print(df['week'].value_counts().sort_index())

print(f"\nCustomer type by week:")
print(df.groupby(['week', 'customer_type']).size())

# Load abandoned logs
abd_path = os.path.join('data', 'combined_abandoned_call_logs.csv')
if os.path.exists(abd_path):
    abd = pd.read_csv(abd_path)
    
    # Deduplicate
    abd = abd.drop_duplicates(subset=['Caller ID', 'Call Time'])
    
    print("\n" + "=" * 60)
    print("ABANDONED LOG ANALYSIS")
    print("=" * 60)
    print(f"Total abandoned (after dedup): {len(abd)}")
    
    # Assign customer type
    trade_numbers = set(df[df['customer_type'] == 'trade']['from_number'].astype(str).apply(lambda x: x.replace('.0', '')).unique())
    print(f"\nTrade numbers in main log: {len(trade_numbers)}")
    
    abd['customer_type'] = abd['Caller ID'].apply(
        lambda x: 'trade' if str(x) in trade_numbers else 'retail'
    )
    
    print(f"\nAbandoned customer type distribution:")
    print(abd['customer_type'].value_counts())
    
    # Calculate week for abandoned using midpoint
    abd['Call Time'] = pd.to_datetime(abd['Call Time'], errors='coerce')
    min_date = df['call_start'].min()
    max_date = df['call_start'].max()
    midpoint = min_date + (max_date - min_date) / 2
    
    print(f"\nMain log date range: {min_date} to {max_date}")
    print(f"Midpoint: {midpoint}")
    
    abd['week'] = abd['Call Time'].apply(lambda x: 1 if x >= midpoint else 2)
    
    print(f"\nAbandoned week distribution:")
    print(abd['week'].value_counts().sort_index())
    
    print(f"\nAbandoned by week and customer type:")
    print(abd.groupby(['week', 'customer_type']).size())
    
    # Calculate metrics like the code does
    week1_abd = abd[abd['week'] == 1]
    week2_abd = abd[abd['week'] == 2]
    
    week1_retail_abd = len(week1_abd[week1_abd['customer_type'] == 'retail'])
    week1_trade_abd = len(week1_abd[week1_abd['customer_type'] == 'trade'])
    week2_retail_abd = len(week2_abd[week2_abd['customer_type'] == 'retail'])
    week2_trade_abd = len(week2_abd[week2_abd['customer_type'] == 'trade'])
    
    total_retail_abd = len(abd[abd['customer_type'] == 'retail'])
    total_trade_abd = len(abd[abd['customer_type'] == 'trade'])
    
    print("\n" + "=" * 60)
    print("CALCULATED METRICS")
    print("=" * 60)
    print(f"Week 1 Retail Abandoned: {week1_retail_abd}")
    print(f"Week 1 Trade Abandoned: {week1_trade_abd}")
    print(f"Week 2 Retail Abandoned: {week2_retail_abd}")
    print(f"Week 2 Trade Abandoned: {week2_trade_abd}")
    print(f"Total Retail Abandoned: {total_retail_abd}")
    print(f"Total Trade Abandoned: {total_trade_abd}")
    
    # Calculate rates
    total_retail_main = len(df[df['customer_type'] == 'retail'])
    total_trade_main = len(df[df['customer_type'] == 'trade'])
    
    retail_rate = (total_retail_abd / (total_retail_main + total_retail_abd) * 100) if (total_retail_main + total_retail_abd) > 0 else 0
    trade_rate = (total_trade_abd / (total_trade_main + total_trade_abd) * 100) if (total_trade_main + total_trade_abd) > 0 else 0
    
    print(f"\nRetail Abandonment Rate: {retail_rate:.1f}%")
    print(f"Trade Abandonment Rate: {trade_rate:.1f}%")
