import pandas as pd
import datetime

def test_logic():
    print("=== CORE LOGIC UNIT TESTS ===")
    
    # 1. Test Phone Cleaning
    print("\n[Test 1] Phone Number Cleaning")
    def clean_phone_for_match(phone):
        s = str(phone).strip()
        s = s.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
        if s.startswith('+353'): s = s[4:]
        elif s.startswith('00353'): s = s[5:]
        elif s.startswith('353'): s = s[3:]
        if s.startswith('0'): s = s[1:]
        return s

    test_cases = [
        ('+353 87 123 4567', '871234567'),
        ('087-123-4567', '871234567'),
        ('00353871234567', '871234567'),
        ('353871234567', '871234567'),
        ('871234567', '871234567'),
        ('(01) 123 4567', '11234567')
    ]
    
    failures = 0
    for input_val, expected in test_cases:
        result = clean_phone_for_match(input_val)
        if result == expected:
            print(f"  PASS: {input_val} -> {result}")
        else:
            print(f"  FAIL: {input_val} -> {result} (Expected: {expected})")
            failures += 1
            
    # 2. Test Week Assignment Logic
    print("\n[Test 2] Week Assignment")
    # Simulate the logic from call_log_analyzer
    max_date = pd.to_datetime("2025-11-30 23:59:59") # Sunday
    week1_start = max_date - pd.Timedelta(days=7) # 2025-11-23 23:59:59
    week1_end = max_date
    week2_start = week1_start - pd.Timedelta(days=7) # 2025-11-16 23:59:59
    week2_end = week1_start
    
    def assign_week(dt_str):
        dt = pd.to_datetime(dt_str)
        if dt > week1_start and dt <= week1_end:
            return 1
        elif dt > week2_start and dt <= week2_end:
            return 2
        else:
            return 3
            
    date_cases = [
        ("2025-11-30 12:00:00", 1), # Week 1 (Last day)
        ("2025-11-24 08:00:00", 1), # Week 1 (First full day)
        ("2025-11-23 23:59:59", 2), # Boundary (Week 2 end / Week 1 start) -> Should be Week 2 based on logic (dt <= week2_end)
        # Wait, week1_start is 23rd 23:59. So 23rd is Week 2. 24th is Week 1. Correct.
        ("2025-11-23 12:00:00", 2), # Week 2
        ("2025-11-17 08:00:00", 2), # Week 2
        ("2025-11-16 12:00:00", 3), # Week 3 (Too old)
    ]
    
    for dt_input, expected_week in date_cases:
        result_week = assign_week(dt_input)
        if result_week == expected_week:
            print(f"  PASS: {dt_input} -> Week {result_week}")
        else:
            print(f"  FAIL: {dt_input} -> Week {result_week} (Expected: {expected_week})")
            failures += 1

    if failures == 0:
        print("\nALL TESTS PASSED ✅")
    else:
        print(f"\n{failures} TESTS FAILED ❌")

if __name__ == "__main__":
    test_logic()
