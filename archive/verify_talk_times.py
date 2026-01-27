import pandas as pd

# Load the cleaned call logs
df = pd.read_csv('reports/call_logs_cleaned.csv')
df['call_start'] = pd.to_datetime(df['call_start'])

print("=" * 80)
print("AVERAGE TALK TIME VERIFICATION")
print("=" * 80)

# Filter for answered calls only (talking_total_sec > 0)
answered = df[df['talking_total_sec'] > 0].copy()

print(f"\nTotal answered calls: {len(answered)}")

# Week 1 Analysis
week1 = answered[answered['week'] == 1]
week1_retail = week1[week1['customer_type'] == 'retail']
week1_trade = week1[week1['customer_type'] == 'trade']

print(f"\n{'='*40}")
print("WEEK 1")
print(f"{'='*40}")
print(f"Date range: {week1['call_start'].min().strftime('%d/%m/%Y')} to {week1['call_start'].max().strftime('%d/%m/%Y')}")
print(f"\nRetail:")
print(f"  Count: {len(week1_retail)}")
print(f"  Total talk seconds: {week1_retail['talking_total_sec'].sum():,.0f}")
print(f"  Average talk time: {week1_retail['talking_total_sec'].mean():.2f} seconds ({week1_retail['talking_total_sec'].mean()/60:.2f} minutes)")
print(f"  Median talk time: {week1_retail['talking_total_sec'].median():.2f} seconds")
print(f"  Min: {week1_retail['talking_total_sec'].min():.0f}s, Max: {week1_retail['talking_total_sec'].max():.0f}s")

print(f"\nTrade:")
print(f"  Count: {len(week1_trade)}")
print(f"  Total talk seconds: {week1_trade['talking_total_sec'].sum():,.0f}")
print(f"  Average talk time: {week1_trade['talking_total_sec'].mean():.2f} seconds ({week1_trade['talking_total_sec'].mean()/60:.2f} minutes)")
print(f"  Median talk time: {week1_trade['talking_total_sec'].median():.2f} seconds")
print(f"  Min: {week1_trade['talking_total_sec'].min():.0f}s, Max: {week1_trade['talking_total_sec'].max():.0f}s")

print(f"\n⚠️  Difference: {abs(week1_retail['talking_total_sec'].mean() - week1_trade['talking_total_sec'].mean()):.2f} seconds")

# Week 2 Analysis
week2 = answered[answered['week'] == 2]
week2_retail = week2[week2['customer_type'] == 'retail']
week2_trade = week2[week2['customer_type'] == 'trade']

print(f"\n{'='*40}")
print("WEEK 2")
print(f"{'='*40}")
print(f"Date range: {week2['call_start'].min().strftime('%d/%m/%Y')} to {week2['call_start'].max().strftime('%d/%m/%Y')}")
print(f"\nRetail:")
print(f"  Count: {len(week2_retail)}")
print(f"  Total talk seconds: {week2_retail['talking_total_sec'].sum():,.0f}")
print(f"  Average talk time: {week2_retail['talking_total_sec'].mean():.2f} seconds ({week2_retail['talking_total_sec'].mean()/60:.2f} minutes)")
print(f"  Median talk time: {week2_retail['talking_total_sec'].median():.2f} seconds")
print(f"  Min: {week2_retail['talking_total_sec'].min():.0f}s, Max: {week2_retail['talking_total_sec'].max():.0f}s")

print(f"\nTrade:")
print(f"  Count: {len(week2_trade)}")
print(f"  Total talk seconds: {week2_trade['talking_total_sec'].sum():,.0f}")
print(f"  Average talk time: {week2_trade['talking_total_sec'].mean():.2f} seconds ({week2_trade['talking_total_sec'].mean()/60:.2f} minutes)")
print(f"  Median talk time: {week2_trade['talking_total_sec'].median():.2f} seconds")
print(f"  Min: {week2_trade['talking_total_sec'].min():.0f}s, Max: {week2_trade['talking_total_sec'].max():.0f}s")

print(f"\n⚠️  Difference: {abs(week2_retail['talking_total_sec'].mean() - week2_trade['talking_total_sec'].mean()):.2f} seconds")

# Sample some records to verify data
print(f"\n{'='*40}")
print("SAMPLE DATA VERIFICATION")
print(f"{'='*40}")
print("\nSample Week 1 Retail calls:")
print(week1_retail[['call_start', 'from_number', 'talking_total_sec']].head(3))
print("\nSample Week 1 Trade calls:")
print(week1_trade[['call_start', 'from_number', 'talking_total_sec']].head(3))
