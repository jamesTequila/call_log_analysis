import pandas as pd
import os
import re

def verify_metrics():
    print("=== METRICS VERIFICATION ===")
    
    # Load Data
    main_df = pd.read_csv('reports/call_logs_cleaned.csv')
    abd_df = pd.read_csv('reports/abandoned_logs_cleaned.csv')
    
    # Filter to Week 1 and Week 2 ONLY
    main_df = main_df[main_df['week'].isin([1, 2])]
    abd_df = abd_df[abd_df['week'].isin([1, 2])]
    
    # Calculate Metrics
    metrics = {}
    
    # 1. Total Calls
    metrics['Total Calls'] = len(main_df) + len(abd_df)
    
    # 2. Week 1 Volume
    w1_main = main_df[main_df['week'] == 1]
    w1_abd = abd_df[abd_df['week'] == 1]
    metrics['Week 1 Volume'] = len(w1_main) + len(w1_abd)
    
    # 3. Week 2 Volume
    w2_main = main_df[main_df['week'] == 2]
    w2_abd = abd_df[abd_df['week'] == 2]
    metrics['Week 2 Volume'] = len(w2_main) + len(w2_abd)
    
    # 4. Week 1 Trade (Answered + Abandoned)
    w1_trade_main = len(w1_main[w1_main['customer_type'] == 'trade'])
    w1_trade_abd = len(w1_abd[w1_abd['customer_type'] == 'trade'])
    metrics['Week 1 Trade'] = w1_trade_main + w1_trade_abd
    
    # 5. Week 2 Trade
    w2_trade_main = len(w2_main[w2_main['customer_type'] == 'trade'])
    w2_trade_abd = len(w2_abd[w2_abd['customer_type'] == 'trade'])
    metrics['Week 2 Trade'] = w2_trade_main + w2_trade_abd
    
    # 6. Week 1 Retail
    w1_retail_main = len(w1_main[w1_main['customer_type'] == 'retail'])
    w1_retail_abd = len(w1_abd[w1_abd['customer_type'] == 'retail'])
    metrics['Week 1 Retail'] = w1_retail_main + w1_retail_abd
    
    # 7. Week 2 Retail
    w2_retail_main = len(w2_main[w2_main['customer_type'] == 'retail'])
    w2_retail_abd = len(w2_abd[w2_abd['customer_type'] == 'retail'])
    metrics['Week 2 Retail'] = w2_retail_main + w2_retail_abd
    
    # 8. Abandonment Counts
    metrics['Week 1 Retail Abandoned'] = len(w1_abd[w1_abd['customer_type'] == 'retail'])
    metrics['Week 1 Trade Abandoned'] = len(w1_abd[w1_abd['customer_type'] == 'trade'])
    metrics['Week 2 Retail Abandoned'] = len(w2_abd[w2_abd['customer_type'] == 'retail'])
    metrics['Week 2 Trade Abandoned'] = len(w2_abd[w2_abd['customer_type'] == 'trade'])
    
    # Generate Markdown Report
    md_content = "# Dashboard vs. Sanity Check Comparison\n\n"
    md_content += "| Metric | Sanity Check Value | Notes |\n"
    md_content += "| :--- | :--- | :--- |\n"
    
    for key, value in metrics.items():
        formatted_value = "{:,}".format(value)
        md_content += f"| {key} | **{formatted_value}** | |\n"
        
    # Add Logic Explanations
    md_content += "\n## Logic Verification\n"
    md_content += "- **Total Calls**: Sum of Week 1 & 2 (Main + Abandoned).\n"
    md_content += "- **Trade/Retail Counts**: Includes BOTH answered and abandoned calls.\n"
    md_content += "- **Week Definition**: Week 1 ends on Global Max Date. Week 2 is the 7 days prior.\n"
    
    output_path = 'sanity/dashboard_comparison.md'
    with open(output_path, 'w') as f:
        f.write(md_content)
        
    print(f"Comparison report generated: {output_path}")
    print(md_content)

if __name__ == "__main__":
    verify_metrics()
