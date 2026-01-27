import os
from jinja2 import Environment, FileSystemLoader
from call_log_analyzer import analyze_calls
from store_snapshot import create_snapshot_table, store_snapshot, create_weekly_metrics_table, store_weekly_metrics

def validate_metrics_quick(metrics, df, abandoned_df):
    """Quick validation of key metrics."""
    errors = []
    
    # Total = Week 1 + Week 2
    w1_w2_sum = metrics['week1_calls'] + metrics['week2_calls']
    if w1_w2_sum != metrics['total_calls']:
        errors.append(f"Total mismatch: {w1_w2_sum} != {metrics['total_calls']}")
    
    # Week 1 = Retail + Trade + Abandoned (as per updated metric definitions)
    w1_abd = metrics.get('week1_retail_abandoned', 0) + metrics.get('week1_trade_abandoned', 0)
    w1_breakdown = metrics['week1_retail_total'] + metrics['week1_trade_total'] + w1_abd
    if w1_breakdown != metrics['week1_calls']:
        errors.append(f"Week 1 breakdown: {w1_breakdown} != {metrics['week1_calls']}")
    
    # Week 2 = Retail + Trade + Abandoned
    w2_abd = metrics.get('week2_retail_abandoned', 0) + metrics.get('week2_trade_abandoned', 0)
    w2_breakdown = metrics['week2_retail_total'] + metrics['week2_trade_total'] + w2_abd
    if w2_breakdown != metrics['week2_calls']:
        errors.append(f"Week 2 breakdown: {w2_breakdown} != {metrics['week2_calls']}")
    
    # Check for extra weeks in data
    if any(w not in [1, 2] for w in df['week'].unique()):
        errors.append(f"Data contains weeks other than 1-2: {sorted(df['week'].unique())}")
    
    return errors

def generate_report():
    # 1. Analyze Data
    # Pass the data directory to analyze_calls
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    print("Running analysis...")
    results = analyze_calls(data_dir)
    
    if not results:
        print("Analysis failed or returned no results.")
        return
    
    # 2. Comprehensive Validation with Historical Tracking
    print("Validating metrics and historical consistency...")
    from validate_historical import validate_report
    from historical_log import log_week_metrics
    from datetime import datetime
    
    # Save verification summary to Markdown file
    os.makedirs('reports', exist_ok=True)
    validation_message, validation_passed = validate_report(results)

    with open('reports/report_verification_summary.md', 'w', encoding='utf-8') as f:
        f.write(validation_message)

    if not validation_passed:
        print("\n" + "="*60)
        print("ERROR: VALIDATION FAILED!")
        print("="*60)
        print(validation_message)
        print("="*60)
        print("Report generation aborted due to validation errors.\n")
        print("Validation summary saved to: reports/report_verification_summary.md")
        return
    
    # 3. Log Historical Week Data
    print("\nLogging historical week data...")
    this_week_data = {
        "start_date": results['metrics']['this_week_start'],
        "end_date": results['metrics']['this_week_end'],
        "retail": results['metrics']['week1_retail_total'],
        "trade": results['metrics']['week1_trade_total'],
        "abandoned": results['metrics'].get('week1_retail_abandoned', 0) + 
                     results['metrics'].get('week1_trade_abandoned', 0),
        "total": results['metrics']['week1_calls']
    }
    
    last_week_data = {
        "start_date": results['metrics']['last_week_start'],
        "end_date": results['metrics']['last_week_end'],
        "retail": results['metrics']['week2_retail_total'],
        "trade": results['metrics']['week2_trade_total'],
        "abandoned": results['metrics'].get('week2_retail_abandoned', 0) + 
                     results['metrics'].get('week2_trade_abandoned', 0),
        "total": results['metrics']['week2_calls']
    }
    
    log_week_metrics(datetime.now(), this_week_data, last_week_data)

    # 4. Store Historical Snapshot (database)
    print("Storing database snapshot...")
    try:
        create_snapshot_table()
        store_snapshot(results['metrics'])
    except Exception as e:
        print(f"Warning: Could not store snapshot: {e}")

    # 4b. Store persistent weekly metrics (keyed on week date range)
    print("Storing weekly metrics to database...")
    try:
        create_weekly_metrics_table()
        store_weekly_metrics(results['metrics'])
    except Exception as e:
        print(f"Warning: Could not store weekly metrics: {e}")

    # 5. Setup Jinja2 Environment
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('call_report.html.j2')
    
    # 6. Render Template
    print("Generating report...")
    html_output = template.render(
        metrics=results['metrics'],
        plots=results['plots'],
        narrative=results['narrative'],
        raw_data=results['raw_data'],
        abandoned_logs=results['abandoned_logs'],
        max_date=results.get('max_date', 'N/A'),
        abandoned_trade_customers=results.get('abandoned_trade_customers', {'week1': [], 'week2': []})
    )
    
    # Save Report
    output_dir = 'reports'
    os.makedirs(output_dir, exist_ok=True)
    
    # Revert to original filename
    output_path = os.path.join(output_dir, 'call_report.html')
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_output)
        print(f"Report generated successfully: {output_path}")
        print("\n" + "="*60)
        print("SUCCESS: Report validated and ready for stakeholders!")
        print("Detailed verification summary saved to: reports/report_verification_summary.md")
        print("="*60)
        print(f"This Week: {this_week_data['start_date']} to {this_week_data['end_date']} ({this_week_data['total']} calls)")
        print(f"Last Week: {last_week_data['start_date']} to {last_week_data['end_date']} ({last_week_data['total']} calls)")
        print("="*60)
            
    except Exception as e:
        print(f"Error writing report: {e}")

if __name__ == "__main__":
    generate_report()
