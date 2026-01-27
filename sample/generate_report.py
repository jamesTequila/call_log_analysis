import os
from jinja2 import Environment, FileSystemLoader
from call_log_analyzer import analyze_calls

def generate_report():
    # 1. Analyze Data
    # Pass the data directory to analyze_calls
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    print("Running analysis...")
    results = analyze_calls(data_dir)
    
    if not results:
        print("Analysis failed or returned no results.")
        return

    # 2. Setup Jinja2 Environment
    # Use template directory relative to this script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(base_dir, 'templates')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('call_report.html.j2')
    
    # 3. Render Template
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
    # Define output directory relative to this script
    output_dir = os.path.join(base_dir, 'reports')
    os.makedirs(output_dir, exist_ok=True)
    
    # Revert to original filename
    output_path = os.path.join(output_dir, 'sample_report.html')
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_output)
        print(f"Report generated successfully: {output_path}")
    except Exception as e:
        print(f"Error writing report: {e}")

if __name__ == "__main__":
    generate_report()
