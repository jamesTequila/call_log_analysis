import pandas as pd

# Load verification data
df = pd.read_csv('reports/verification_data.csv')
df['call_start'] = pd.to_datetime(df['call_start'])

# Get max date
max_date = df['call_start'].max()

# Get week data
w1 = df[df['week'] == 1]
w2 = df[df['week'] == 2]

print("=" * 60)
print("WEEK ASSIGNMENT SUMMARY")
print("=" * 60)
print(f"\nMax date in dataset: {max_date.strftime('%d/%m/%Y %H:%M:%S')}")
print(f"\nWeek 1 (Last 7 days from max date):")
print(f"  Date range: {w1['call_start'].min().strftime('%d/%m/%Y')} to {w1['call_start'].max().strftime('%d/%m/%Y')}")
print(f"  Total calls: {len(w1):,}")
print(f"  Retail: {len(w1[w1['customer_type']=='retail']):,}")
print(f"  Trade: {len(w1[w1['customer_type']=='trade']):,}")

print(f"\nWeek 2 (7 days before Week 1):")
print(f"  Date range: {w2['call_start'].min().strftime('%d/%m/%Y')} to {w2['call_start'].max().strftime('%d/%m/%Y')}")
print(f"  Total calls: {len(w2):,}")
print(f"  Retail: {len(w2[w2['customer_type']=='retail']):,}")
print(f"  Trade: {len(w2[w2['customer_type']=='trade']):,}")

print("\n" + "=" * 60)
print(f"Report location: reports/call_report.html")
print("=" * 60)
