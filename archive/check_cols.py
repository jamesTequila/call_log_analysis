import pandas as pd
try:
    df = pd.read_csv('reports/call_logs_cleaned.csv')
    print("Call Logs Columns:")
    for c in df.columns:
        print(f"- {c}")
except:
    print("Call Logs not found")

try:
    df = pd.read_csv('reports/abandoned_logs_cleaned.csv')
    print("\nAbandoned Logs Columns:")
    for c in df.columns:
        print(f"- {c}")
except:
    print("Abandoned Logs not found")
