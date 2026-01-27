import pandas as pd

# Load the cleaned call logs
df = pd.read_csv('reports/call_logs_cleaned.csv')
df['call_start'] = pd.to_datetime(df['call_start'])

# Filter for answered calls only
answered = df[df['talking_total_sec'] > 0].copy()

results = []
results.append("=" * 80)
results.append("AVERAGE TALK TIME VERIFICATION")
results.append("=" * 80)

# Week 1
week1 = answered[answered['week'] == 1]
week1_retail = week1[week1['customer_type'] == 'retail']
week1_trade = week1[week1['customer_type'] == 'trade']

results.append("\nWEEK 1")
results.append(f"Retail - Count: {len(week1_retail)}, Avg: {week1_retail['talking_total_sec'].mean():.2f}s ({week1_retail['talking_total_sec'].mean()/60:.2f} min)")
results.append(f"Trade  - Count: {len(week1_trade)}, Avg: {week1_trade['talking_total_sec'].mean():.2f}s ({week1_trade['talking_total_sec'].mean()/60:.2f} min)")
results.append(f"Difference: {abs(week1_retail['talking_total_sec'].mean() - week1_trade['talking_total_sec'].mean()):.2f} seconds")

# Week 2
week2 = answered[answered['week'] == 2]
week2_retail = week2[week2['customer_type'] == 'retail']
week2_trade = week2[week2['customer_type'] == 'trade']

results.append("\nWEEK 2")
results.append(f"Retail - Count: {len(week2_retail)}, Avg: {week2_retail['talking_total_sec'].mean():.2f}s ({week2_retail['talking_total_sec'].mean()/60:.2f} min)")
results.append(f"Trade  - Count: {len(week2_trade)}, Avg: {week2_trade['talking_total_sec'].mean():.2f}s ({week2_trade['talking_total_sec'].mean()/60:.2f} min)")
results.append(f"Difference: {abs(week2_retail['talking_total_sec'].mean() - week2_trade['talking_total_sec'].mean()):.2f} seconds")

# Write to file
with open('talk_time_verification.txt', 'w') as f:
    f.write('\n'.join(results))

print("Results saved to talk_time_verification.txt")
for line in results:
    print(line)
