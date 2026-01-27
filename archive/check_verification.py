import pandas as pd

# Check verification data
df = pd.read_csv('reports/verification_data.csv')

print("=" * 80)
print("VERIFICATION DATA ANALYSIS")
print("=" * 80)

# Week distribution
print("\nWeek Distribution:")
week_counts = df['week'].value_counts().sort_index()
for week, count in week_counts.items():
    print(f"  Week {int(week)}: {count} calls")

# Week 2 analysis
w2 = df[df['week'] == 2]
if len(w2) > 0:
    df['call_start'] = pd.to_datetime(df['call_start'])
    w2_dates = df[df['week'] == 2]['call_start']
    print(f"\n✅ Week 2 EXISTS:")
    print(f"   Date Range: {w2_dates.min()} to {w2_dates.max()}")
    print(f"   Total Calls: {len(w2)}")
    
    # Customer breakdown
    w2_retail = len(w2[w2['customer_type'] == 'retail'])
    w2_trade = len(w2[w2['customer_type'] == 'trade'])
    print(f"   Retail: {w2_retail}, Trade: {w2_trade}")
else:
    print(f"\n❌ Week 2 is EMPTY - this is the problem!")

# Week 1 analysis
w1 = df[df['week'] == 1]
if len(w1) > 0:
    df['call_start'] = pd.to_datetime(df['call_start'])
    w1_dates = df[df['week'] == 1]['call_start']
    print(f"\n✅ Week 1:")
    print(f"   Date Range: {w1_dates.min()} to {w1_dates.max()}")
    print(f"   Total Calls: {len(w1)}")
