import pandas as pd

# Load verification data
df = pd.read_csv('reports/verification_data.csv')
df['call_start'] = pd.to_datetime(df['call_start'])

# Get max date
max_date = df['call_start'].max()

# Get week data
w1 = df[df['week'] == 1]
w2 = df[df['week'] == 2]
w3 = df[df['week'] == 3]

print("=" * 80)
print("FINAL WEEK ASSIGNMENT VERIFICATION")
print("=" * 80)
print(f"\nMax date in dataset: {max_date.strftime('%d/%m/%Y %H:%M:%S')}")
print(f"Expected Week 1: {(max_date - pd.Timedelta(days=7)).strftime('%d/%m/%Y')} to {max_date.strftime('%d/%m/%Y')}")

print(f"\n✅ ACTUAL Week 1:")
print(f"   Date range: {w1['call_start'].min().strftime('%d/%m/%Y %H:%M')} to {w1['call_start'].max().strftime('%d/%m/%Y %H:%M')}")
print(f"   Total calls: {len(w1):,}")
print(f"   - Retail: {len(w1[w1['customer_type']=='retail']):,}")
print(f"   - Trade: {len(w1[w1['customer_type']=='trade']):,}")

if len(w2) > 0:
    print(f"\n✅ ACTUAL Week 2:")
    print(f"   Date range: {w2['call_start'].min().strftime('%d/%m/%Y %H:%M')} to {w2['call_start'].max().strftime('%d/%m/%Y %H:%M')}")
    print(f"   Total calls: {len(w2):,}")
    print(f"   - Retail: {len(w2[w2['customer_type']=='retail']):,}")
    print(f"   - Trade: {len(w2[w2['customer_type']=='trade']):,}")

if len(w3) > 0:
    print(f"\n✅ ACTUAL Week 3:")
    print(f"   Date range: {w3['call_start'].min().strftime('%d/%m/%Y %H:%M')} to {w3['call_start'].max().strftime('%d/%m/%Y %H:%M')}")
    print(f"   Total calls: {len(w3):,}")

print("\n" + "=" * 80)
print(f"Report generated: reports/call_report.html")
print("=" * 80)
