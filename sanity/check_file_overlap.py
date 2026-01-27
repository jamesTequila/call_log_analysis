import csv
import os
import sys

print("Starting overlap check (CSV mode)...", flush=True)

file_1201 = os.path.join('data', 'CallLogLastWeek_1201_mWtBCfAskIyaqhZe22c1.csv')
file_1901 = os.path.join('data', 'CallLogLastWeek_1901_46eIDBjWKLIigqcwo0fy.csv')

def get_date_range(path):
    print(f"Reading {os.path.basename(path)}...", flush=True)
    dates = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ct = row.get('Call Time', '').strip()
                # Simple ISO cutoff (YYYY-MM-DDTHH:MM:SS)
                if ct and ct != 'Totals' and len(ct) >= 19:
                    dates.append(ct[:19])
    except Exception as e:
        print(f"Error: {e}")
        return None, None, []
        
    if not dates:
        return None, None, []
        
    dates.sort()
    return dates[0], dates[-1], dates

min1, max1, dates1 = get_date_range(file_1201)
min2, max2, dates2 = get_date_range(file_1901)

if min1 and min2:
    print(f"\nFile 1 (Old): {min1} to {max1} ({len(dates1)} rows)")
    print(f"File 2 (New): {min2} to {max2} ({len(dates2)} rows)")
    
    # Check string comparison overlap
    # Note: Text comparison works for ISO dates
    latest_start = max(min1, min2)
    earliest_end = min(max1, max2)
    
    if latest_start < earliest_end:
        print(f"\nOVERLAP DETECTED: {latest_start} to {earliest_end}")
        
        # Find overlapping rows (exact string match)
        set1 = set(d for d in dates1 if d >= latest_start and d <= earliest_end)
        set2 = set(d for d in dates2 if d >= latest_start and d <= earliest_end)
        
        print(f" timestamps in file 1 overlap: {len(set1)}")
        print(f" timestamps in file 2 overlap: {len(set2)}")
        
        diff = set1.symmetric_difference(set2)
        if diff:
            print(f"DIFFERENCE count: {len(diff)}")
            print("First few diffs:", list(diff)[:5])
        else:
            print("Overlap matches perfectly.")
            
    else:
        print("\nNO OVERLAP DETECTED.")
