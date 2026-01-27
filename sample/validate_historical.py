"""
Comprehensive Report Validation Module

Validates report consistency:
1. Arithmetic: Retail + Trade + Abandoned = Total
2. Week-to-week historical consistency
3. No date overlaps between weeks
"""
from datetime import datetime, timedelta

def validate_arithmetic(metrics):
    """
    Verify that all arithmetic adds up correctly
    
    Returns:
        tuple: (is_valid: bool, errors: list)
    """
    errors = []
    
    # Check: Total = This Week + Last Week
    expected_total = metrics['week1_calls'] + metrics['week2_calls']
    if expected_total != metrics['total_calls']:
        errors.append(
            f"Total mismatch: {metrics['week1_calls']} + {metrics['week2_calls']} "
            f"= {expected_total} != {metrics['total_calls']}"
        )
    
    # Check: This Week = Retail + Trade + Abandoned
    this_week_retail = metrics['week1_retail_total']
    this_week_trade = metrics['week1_trade_total']
    this_week_abd = metrics.get('week1_retail_abandoned', 0) + metrics.get('week1_trade_abandoned', 0)
    this_week_calc = this_week_retail + this_week_trade + this_week_abd
    
    if this_week_calc != metrics['week1_calls']:
        errors.append(
            f"This Week breakdown mismatch: {this_week_retail} + {this_week_trade} + {this_week_abd} "
            f"= {this_week_calc} != {metrics['week1_calls']}"
        )
    
    # Check: Last Week = Retail + Trade + Abandoned
    last_week_retail = metrics['week2_retail_total']
    last_week_trade = metrics['week2_trade_total']
    last_week_abd = metrics.get('week2_retail_abandoned', 0) + metrics.get('week2_trade_abandoned', 0)
    last_week_calc = last_week_retail + last_week_trade + last_week_abd
    
    if last_week_calc != metrics['week2_calls']:
        errors.append(
            f"Last Week breakdown mismatch: {last_week_retail} + {last_week_trade} + {last_week_abd} "
            f"= {last_week_calc} != {metrics['week2_calls']}"
        )
    
    return (len(errors) == 0, errors)

def validate_date_ranges(metrics):
    """
    Verify no date overlap between weeks
    
    Returns:
        tuple: (is_valid: bool, errors: list)
    """
    errors = []
    
    this_week_start = datetime.strptime(metrics['this_week_start'], '%Y-%m-%d')
    this_week_end = datetime.strptime(metrics['this_week_end'], '%Y-%m-%d')
    last_week_start = datetime.strptime(metrics['last_week_start'], '%Y-%m-%d')
    last_week_end = datetime.strptime(metrics['last_week_end'], '%Y-%m-%d')
    
    # Check: Last Week ends BEFORE This Week starts (no overlap)
    if last_week_end >= this_week_start:
        errors.append(
            f"Week overlap detected:\n"
            f"  Last Week: {metrics['last_week_start']} to {metrics['last_week_end']}\n"
            f"  This Week: {metrics['this_week_start']} to {metrics['this_week_end']}\n"
            f"  Last Week should end before This Week starts!"
        )
    
    # Check: Weeks are exactly 7 days
    this_week_days = (this_week_end - this_week_start).days + 1
    last_week_days = (last_week_end - last_week_start).days + 1
    
    if this_week_days != 7:
        errors.append(f"This Week is {this_week_days} days (should be 7)")
    
    if last_week_days != 7:
        errors.append(f"Last Week is {last_week_days} days (should be 7)")
    
    return (len(errors) == 0, errors)

def validate_report(results):
    """
    Run all validation checks on report results
    
    Args:
        results: Dict returned from analyze_calls()
        
    Returns:
        tuple: (is_valid: bool, summary: str)
    """
    metrics = results['metrics']
    
    print("\n" + "="*70)
    print("REPORT VALIDATION")
    print("="*70)
    
    all_valid = True
    messages = []
    
    # 1. Arithmetic validation
    arith_valid, arith_errors = validate_arithmetic(metrics)
    if arith_valid:
        messages.append("[OK] Arithmetic validation passed")
    else:
        all_valid = False
        messages.append("[FAIL] Arithmetic validation failed:")
        for error in arith_errors:
            messages.append(f"  - {error}")
    
    # 2. Date range validation
    date_valid, date_errors = validate_date_ranges(metrics)
    if date_valid:
        messages.append("[OK] Date range validation passed")
        messages.append(f"  This Week: {metrics['this_week_start']} to {metrics['this_week_end']}")
        messages.append(f"  Last Week: {metrics['last_week_start']} to {metrics['last_week_end']}")
    else:
        all_valid = False
        messages.append("[FAIL] Date range validation failed:")
        for error in date_errors:
            messages.append(f"  - {error}")
    
    # 3. Historical consistency (if applicable)
    from historical_log import verify_historical_consistency
    
    last_week_data = {
        "start_date": metrics['last_week_start'],
        "end_date": metrics['last_week_end'],
        "retail": metrics['week2_retail_total'],
        "trade": metrics['week2_trade_total'],
        "abandoned": metrics.get('week2_retail_abandoned', 0) + metrics.get('week2_trade_abandoned', 0),
        "total": metrics['week2_calls']
    }
    
    hist_valid, hist_message = verify_historical_consistency(last_week_data)
    messages.append(hist_message)
    if not hist_valid:
        all_valid = False
    
    # Print summary
    print("\n".join(messages))
    print("="*70)
    
    if all_valid:
        print("[PASS] All validation checks passed!")
        return True, "\n".join(messages)
    else:
        print("[FAIL] Validation failed - report may be inconsistent")
        return False, "\n".join(messages)
