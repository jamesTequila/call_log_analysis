import pandas as pd
import os

def generate_audit():
    print("=== GENERATING AUDIT SAMPLE ===")
    
    main_df = pd.read_csv('reports/call_logs_cleaned.csv')
    abd_df = pd.read_csv('reports/abandoned_logs_cleaned.csv')
    
    # Sample 20 from Week 1 (Main)
    s1 = main_df[main_df['week'] == 1].sample(n=min(20, len(main_df[main_df['week'] == 1])))
    s1['Source'] = 'Main Log'
    
    # Sample 20 from Week 2 (Main)
    s2 = main_df[main_df['week'] == 2].sample(n=min(20, len(main_df[main_df['week'] == 2])))
    s2['Source'] = 'Main Log'
    
    # Sample 10 Abandoned
    s3 = abd_df.sample(n=min(10, len(abd_df)))
    s3['Source'] = 'Abandoned Log'
    # Rename columns to match
    s3 = s3.rename(columns={'Call Time': 'call_start', 'Caller ID': 'from_number'})
    
    # Combine
    audit_df = pd.concat([s1, s2, s3], ignore_index=True)
    
    # Select useful columns
    cols = ['call_start', 'from_number', 'customer_type', 'week', 'Source']
    # Add extra context if available
    if 'call_activity_details' in audit_df.columns:
        cols.append('call_activity_details')
        
    audit_df = audit_df[cols]
    
    output_path = 'sanity/audit_sample.csv'
    audit_df.to_csv(output_path, index=False)
    print(f"Audit sample generated: {output_path}")
    print(f"Rows: {len(audit_df)}")

if __name__ == "__main__":
    generate_audit()
