"""
Extract trade customer phone numbers and names for verification.
This script reads the combined call logs and extracts unique trade customer details.
"""
import pandas as pd
import re

# Read the cleaned call logs
df = pd.read_csv('reports/call_logs_cleaned.csv')

# Filter for trade customers
trade_customers = df[df['customer_type'] == 'trade'].copy()

def extract_customer_name(activity_details):
    """Extract customer name from call activity details."""
    if pd.isna(activity_details) or activity_details == '':
        return 'Unknown'
    
    # Look for pattern "Inbound: [Name]" at start of activity details
    match = re.search(r'Inbound:\s*([^→(]+)', str(activity_details))
    if match:
        name = match.group(1).strip()
        # Clean up common patterns
        name = name.split('|')[0].strip()  # Take first part if multiple segments
        name = name.split('→')[0].strip()  # Remove arrow and everything after
        
        # Filter out phone numbers (if name is all digits)
        if name.replace(' ', '').replace('.', '').replace('-', '').isdigit():
            return 'Unknown'
        
        return name if name else 'Unknown'
    
    return 'Unknown'

# Extract names
trade_customers['customer_name'] = trade_customers['call_activity_details'].apply(extract_customer_name)

# Group by phone number and get the most common name for each
trade_summary = trade_customers.groupby('from_number').agg({
    'customer_name': lambda x: x.value_counts().index[0] if len(x) > 0 else 'Unknown'
}).reset_index()

# Filter out "anonymous" entries (retail customers misclassified as trade)
trade_summary = trade_summary[
    ~trade_summary['customer_name'].str.lower().str.contains('anonymous', na=False)
]

# Rename columns
trade_summary.columns = ['phone_number', 'customer_name']

# Clean phone numbers - remove +353 country code
def clean_phone_number(phone):
    """Clean phone number by removing +353 and standardizing format."""
    phone_str = str(phone).strip()
    
    # Remove +353 country code
    if phone_str.startswith('+353'):
        phone_str = phone_str.replace('+353', '0', 1)
    elif phone_str.startswith('353'):
        phone_str = '0' + phone_str[3:]
    
    # Remove spaces, dashes, parentheses
    phone_str = phone_str.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    # Remove decimal point if present (from float conversion)
    if '.' in phone_str:
        phone_str = phone_str.split('.')[0]
    
    return phone_str

trade_summary['phone_number'] = trade_summary['phone_number'].apply(clean_phone_number)

# Sort by customer name
trade_summary = trade_summary.sort_values('customer_name')

# Save to data folder
output_path = 'data/trade_customer_numbers.csv'
trade_summary.to_csv(output_path, index=False)

print(f"✅ Extracted {len(trade_summary)} unique trade customers")
print(f"✅ Filtered out 'anonymous' entries")
print(f"✅ Saved to: {output_path}")
print(f"\nSample entries:")
print(trade_summary.head(10).to_string(index=False))
print(f"\n... and {max(0, len(trade_summary) - 10)} more entries")
