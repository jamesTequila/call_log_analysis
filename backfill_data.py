import os
import re
import glob
from weekly_data_manager import save_week_data, initialize_db

REPORTS_DIR = 'reports'

def extract_metrics_from_report(report_path):
    """Extract usage metrics from an HTML report."""
    with open(report_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Remove script and style elements
    content = re.sub(r'<(script|style)[^>]*>[\s\S]*?</\1>', '', content, flags=re.IGNORECASE)
    
    # 2. Remove HTML tags
    content_clean = re.sub(r'<[^>]+>', ' ', content)
    
    # 3. Collapse whitespace
    content_clean = re.sub(r'\s+', ' ', content_clean)
    
def extract_section_metrics(content_clean, section_name):
    """Refactored parsing for a specific section (This Week or Last Week)."""
    # Regex Pattern: "SectionName (DD/MM/YYYY to DD/MM/YYYY): Received X,XXX calls total."
    pattern = re.compile(
        re.escape(section_name) + 
        r'\s*\(\s*(\d{2}/\d{2}/\d{4})\s*to\s*(\d{2}/\d{2}/\d{4})\s*\)\s*:\s*Received\s*([\d,]+)\s*calls',
        re.IGNORECASE
    )
    
    match = pattern.search(content_clean)
    if not match:
        return None
    
    start_date = match.group(1)
    end_date = match.group(2)
    total_calls = int(match.group(3).replace(',', ''))
    
    # Search for breakdown in the vicinity
    start_idx = match.end()
    # Limit search to avoid bleeding into next section
    search_window = content_clean[start_idx:start_idx+1500]
    
    retail_match = re.search(r'-\s*Retail:\s*([\d,]+)\s*calls', search_window)
    trade_match = re.search(r'-\s*Trade:\s*([\d,]+)\s*calls', search_window)
    abandoned_match = re.search(r'-\s*Abandoned:\s*([\d,]+)\s*calls', search_window)
    
    retail_calls = int(retail_match.group(1).replace(',', '')) if retail_match else 0
    trade_calls = int(trade_match.group(1).replace(',', '')) if trade_match else 0
    abandoned_total = int(abandoned_match.group(1).replace(',', '')) if abandoned_match else 0
    
    # Extract breakdown if available: "(Retail: 274, Trade: 25)"
    abandoned_split_match = re.search(r'\(\s*Retail:\s*([\d,]+),\s*Trade:\s*([\d,]+)\s*\)', search_window)
    
    if abandoned_split_match:
        abandoned_retail = int(abandoned_split_match.group(1).replace(',', ''))
        abandoned_trade = int(abandoned_split_match.group(2).replace(',', ''))
    else:
        # Fallback for legacy: Assign total to retail
        abandoned_retail = abandoned_total
        abandoned_trade = 0
        
    return {
        'start_date': start_date,
        'end_date': end_date,
        'total': total_calls,
        'retail': retail_calls,
        'trade': trade_calls,
        'abandoned': abandoned_total,
        'abandoned_retail': abandoned_retail,
        'abandoned_trade': abandoned_trade
    }

def extract_metrics_from_report(report_path):
    """Extract usage metrics from an HTML report."""
    with open(report_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Remove script and style elements
    content = re.sub(r'<(script|style)[^>]*>[\s\S]*?</\1>', '', content, flags=re.IGNORECASE)
    
    # 2. Remove HTML tags
    content_clean = re.sub(r'<[^>]+>', ' ', content)
    
    # 3. Collapse whitespace
    content_clean = re.sub(r'\s+', ' ', content_clean)
    
    results = []
    
    # Extract This Week
    this_week = extract_section_metrics(content_clean, "This Week")
    if this_week:
        results.append(this_week)
        
    # Extract Last Week
    last_week = extract_section_metrics(content_clean, "Last Week")
    if last_week:
        results.append(last_week)
        
    return results

def main():
    print("Starting backfill process...")
    initialize_db()
    
    # Get all html reports
    report_files = glob.glob(os.path.join(REPORTS_DIR, 'call_report_*.html'))
    
    # Sort by filename (which contains date usually) to insert in order
    # Assuming format call_report_DD_MM_YYYY.html
    # We can try to sort by the date in filename
    def get_date_from_filename(fname):
        try:
            date_str = re.search(r'call_report_(\d{2}_\d{2}_\d{4})\.html', fname).group(1)
            return datetime.strptime(date_str, '%d_%m_%Y')
        except:
            return datetime.min

    from datetime import datetime
    report_files.sort(key=get_date_from_filename)
    
    count = 0
    for report_path in report_files:
        print(f"Processing {os.path.basename(report_path)}...")
        metrics_list = extract_metrics_from_report(report_path)
        
        if metrics_list:
            for metrics in metrics_list:
                print(f"  Found data: {metrics['start_date']} - {metrics['end_date']}")
                print(f"  Total: {metrics['total']} (R: {metrics['retail']}, T: {metrics['trade']}, Abd: {metrics['abandoned']})")
                save_week_data(metrics)
                count += 1
        else:
            print("  No metrics extracted.")
            
    print(f"\nBackfill complete. Processed {count} reports.")

if __name__ == "__main__":
    main()
