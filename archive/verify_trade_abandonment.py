"""
Verify trade abandonment by matching abandoned logs against known trade numbers.
"""
import pandas as pd

def clean_phone_number(phone):
    """Clean phone number to match trade list format."""
    phone_str = str(phone).strip()
    # Remove +353 country code
    if phone_str.startswith('+353'):
        phone_str = phone_str.replace('+353', '0', 1)
    elif phone_str.startswith('353'):
        phone_str = '0' + phone_str[3:]
    
    # Remove spaces, dashes, parentheses
    phone_str = phone_str.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    # Remove decimal point if present
    if '.' in phone_str:
        phone_str = phone_str.split('.')[0]
        
    return phone_str

print("="*60)
print("TRADE ABANDONMENT VERIFICATION")
print("="*60)

# 1. Load Trade Numbers
try:
    trade_df = pd.read_csv('data/trade_customer_numbers.csv')
    # Ensure phone numbers are strings and cleaned
    trade_df['phone_number'] = trade_df['phone_number'].apply(clean_phone_number)
    trade_numbers = set(trade_df['phone_number'].unique())
    print(f"Loaded {len(trade_numbers)} unique trade numbers.")
except Exception as e:
    print(f"Error loading trade numbers: {e}")
    exit(1)

# 2. Load Abandoned Logs
try:
    # Try to load cleaned logs first as they have week info
    abd_df = pd.read_csv('reports/abandoned_logs_cleaned.csv')
    print(f"Loaded {len(abd_df)} abandoned calls.")
except FileNotFoundError:
    print("reports/abandoned_logs_cleaned.csv not found.")
    exit(1)

# 3. Match and Verify
# Clean caller IDs in abandoned logs
abd_df['clean_caller_id'] = abd_df['Caller ID'].apply(clean_phone_number)

# Find matches
trade_matches = abd_df[abd_df['clean_caller_id'].isin(trade_numbers)].copy()

print("\nMATCHING RESULTS:")
print(f"Found {len(trade_matches)} abandoned calls from known trade numbers.")

if not trade_matches.empty:
    # Group by week
    print("\nBreakdown by Week:")
    week_counts = trade_matches['week'].value_counts().sort_index()
    for week, count in week_counts.items():
        print(f"  Week {week}: {count} calls")
        
    print("\nDetailed Matches:")
    # Merge with trade names for better detail
    detailed = trade_matches.merge(
        trade_df, 
        left_on='clean_caller_id', 
        right_on='phone_number', 
        how='left'
    )
    
    # Show columns: Week, Call Time, Customer Name, Phone Number
    display_cols = ['week', 'Call Time', 'customer_name', 'phone_number']
    print(detailed[display_cols].sort_values(['week', 'Call Time']).to_string(index=False))
else:
    print("\n✅ No trade customers found in abandoned logs.")

print("\n" + "="*60)
