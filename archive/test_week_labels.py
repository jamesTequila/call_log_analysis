import pandas as pd
from call_log_analyzer import get_week_date_label

# Load verification data to get max date
df = pd.read_csv('reports/verification_data.csv')
df['call_start'] = pd.to_datetime(df['call_start'])
max_date = df['call_start'].max()

print("=" * 80)
print("WEEK LABEL VERIFICATION")
print("=" * 80)
print(f"\nMax Date: {max_date.strftime('%d/%m/%Y')}")

# Test get_week_date_label function
week1_label = get_week_date_label(1, max_date)
week2_label = get_week_date_label(2, max_date)

print(f"\nWeek 1 Label: {week1_label}")
print(f"Week 2 Label: {week2_label}")

# Expected values
week1_start_expected = max_date - pd.Timedelta(days=6)  # 7 days total
week2_start_expected = max_date - pd.Timedelta(days=13)

print(f"\nExpected Week 1 start: {week1_start_expected.strftime('%d/%m/%Y')}")
print(f"Expected Week 2 start: {week2_start_expected.strftime('%d/%m/%Y')}")

# Verify actual data
w1 = df[df['week'] == 1]
w2 = df[df['week'] == 2]

print(f"\n✅ Actual Week 1 date range: {w1['call_start'].min().strftime('%d/%m/%Y')} to {w1['call_start'].max().strftime('%d/%m/%Y')}")
print(f"✅ Actual Week 2 date range: {w2['call_start'].min().strftime('%d/%m/%Y')} to {w2['call_start'].max().strftime('%d/%m/%Y')}")
