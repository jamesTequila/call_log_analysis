import pandas as pd
import os
import sys
import re

# Logic from cleaning.py, but with STRICT default (mimicking hypothetical old version)

def parse_hms_to_seconds(s: str) -> int:
    try:
        parts = str(s).split(":")
        if len(parts) != 3: return 0
        h, m, sec = map(int, parts)
        return h * 3600 + m * 60 + sec
    except Exception: return 0

def classify_customer_from_activity(activity: str) -> str | None:
    if not isinstance(activity, str): return None
    m = re.search(r"Inbound:\s*(.+?)(?:\s*→|\s*\(|$)", activity)
    if not m: return None
    token = m.group(1).strip()
    if not token: return None
    first = token[0]
    if first.isdigit(): return "retail"
    else: return "trade"

# Modified Resolution Logic: STRICT
def resolve_customer_type_strict(series: pd.Series) -> str:
    vals = set(v for v in series if isinstance(v, str))
    if "trade" in vals: return "trade"
    if "retail" in vals: return "retail"
    return "unknown" # OLD LOGIC HYPOTHESIS

def clean_and_classify_strict(file_path):
    df = pd.read_csv(file_path)
    df["Call Time dt"] = pd.to_datetime(df["Call Time"], errors="coerce")
    df = df[~df["Call Time dt"].isna()].copy()
    
    df["Ringing_sec"] = df["Ringing"].apply(parse_hms_to_seconds)
    df["Talking_sec"] = df["Talking"].apply(parse_hms_to_seconds)
    
    df["customer_type_leg"] = df["Call Activity Details"].apply(classify_customer_from_activity)
    
    # Filter Inbound
    df = df[df["Direction"].isin(["Inbound", "Inbound Queue"])].copy()
    
    # Group
    grouped = df.groupby("Call ID").agg(
        call_start=("Call Time dt", "min"),
        customer_type=("customer_type_leg", resolve_customer_type_strict)
    ).reset_index()
    
    return grouped

# Test
file_path = os.path.join('data', 'CallLogLastWeek_1201_mWtBCfAskIyaqhZe22c1.csv')
print(f"Testing STRICT logic on {file_path}...")

df = clean_and_classify_strict(file_path)

# Filter Week (Jan 5-11)
start = pd.Timestamp('2026-01-05')
end = pd.Timestamp('2026-01-11 23:59:59')
df = df[(df['call_start'] >= start) & (df['call_start'] <= end)]

print(f"Total Rows: {len(df)}")
counts = df['customer_type'].value_counts()
print(counts)

retail = counts.get('retail', 0)
trade = counts.get('trade', 0)
total_valid = retail + trade
print(f"\nTotal Valid (Retail+Trade): {total_valid}")
print("Compare to Old Report: 1519")
