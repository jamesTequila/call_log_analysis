"""
Historical Week Tracking Module

Stores historical week data to enable verification that the same calendar
week shows the same metrics across different report runs.
"""
import json
import os
from datetime import datetime
from pathlib import Path

HISTORICAL_LOG_PATH = 'reports/historical_weeks.json'

def ensure_log_exists():
    """Create log file if it doesn't exist"""
    if not os.path.exists(HISTORICAL_LOG_PATH):
        Path(HISTORICAL_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(HISTORICAL_LOG_PATH, 'w') as f:
            json.dump({"reports": []}, f, indent=2)

def log_week_metrics(report_date, this_week_data, last_week_data):
    """
    Log metrics for This Week and Last Week
    
    Args:
        report_date: Date the report was generated
        this_week_data: Dict with start_date, end_date, retail, trade, abandoned, total
        last_week_data: Dict with start_date, end_date, retail, trade, abandoned, total
    """
    ensure_log_exists()
    
    with open(HISTORICAL_LOG_PATH, 'r') as f:
        data = json.load(f)
    
    # Create new entry
    entry = {
        "report_date": report_date.strftime('%Y-%m-%d'),
        "this_week": this_week_data,
        "last_week": last_week_data
    }
    
    data["reports"].append(entry)
    
    with open(HISTORICAL_LOG_PATH, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n[OK] Historical metrics logged to {HISTORICAL_LOG_PATH}")

def get_previous_report():
    """
    Get the most recent previous report entry
    
    Returns:
        Dict with 'this_week' and 'last_week' data, or None if no previous report
    """
    if not os.path.exists(HISTORICAL_LOG_PATH):
        return None
    
    with open(HISTORICAL_LOG_PATH, 'r') as f:
        data = json.load(f)
    
    if not data["reports"]:
        return None
    
    # Return most recent (last) report
    return data["reports"][-1]

def verify_historical_consistency(current_last_week):
    """
    Verify that current 'Last Week' matches previous 'This Week'
    
    Args:
        current_last_week: Dict with start_date, end_date, retail, trade, abandoned, total
        
    Returns:
        tuple: (is_consistent: bool, message: str)
    """
    previous_report = get_previous_report()
    
    if not previous_report:
        return True, "No previous report to compare against (first run)"
    
    previous_this_week = previous_report["this_week"]
    
    # Check date ranges match
    if (current_last_week["start_date"] != previous_this_week["start_date"] or
        current_last_week["end_date"] != previous_this_week["end_date"]):
        return False, (
            f"Date mismatch:\n"
            f"  Previous 'This Week': {previous_this_week['start_date']} to {previous_this_week['end_date']}\n"
            f"  Current 'Last Week':  {current_last_week['start_date']} to {current_last_week['end_date']}"
       )
    
    # Check metrics match
    metrics_to_check = ['retail', 'trade', 'abandoned', 'total']
    mismatches = []
    
    for metric in metrics_to_check:
        prev_val = previous_this_week.get(metric, 0)
        curr_val = current_last_week.get(metric, 0)
        
        if prev_val != curr_val:
            mismatches.append(
                f"  {metric.capitalize()}: {prev_val} (previous) != {curr_val} (current)"
            )
    
    if mismatches:
        return False, "Metric mismatch:\n" + "\n".join(mismatches)
    
    return True, (
        f"[OK] Historical consistency verified\n"
        f"  Week: {current_last_week['start_date']} to {current_last_week['end_date']}\n"
        f"  All metrics match previous report"
    )

def get_historical_summary():
    """Get a summary of all historical reports"""
    if not os.path.exists(HISTORICAL_LOG_PATH):
        return "No historical data available"
    
    with open(HISTORICAL_LOG_PATH, 'r') as f:
        data = json.load(f)
    
    if not data["reports"]:
        return "No reports logged yet"
    
    summary = ["Historical Reports Summary:"]
    summary.append("=" * 70)
    
    for report in data["reports"]:
        summary.append(f"\nReport Date: {report['report_date']}")
        summary.append(f"  This Week ({report['this_week']['start_date']} to {report['this_week']['end_date']}): {report['this_week']['total']} calls")
        summary.append(f"  Last Week ({report['last_week']['start_date']} to {report['last_week']['end_date']}): {report['last_week']['total']} calls")
    
    return "\n".join(summary)
