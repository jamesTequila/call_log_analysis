import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import re

# Database configuration
DB_CONFIG = {
    'host': 'kmc.tequila-ai.com',
    'port': '5432',
    'database': 'tequila_ai_reporting',
    'user': 'james',
    'password': ']dT1H-{ekquGfn^6',
    'sslmode': 'require'
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def clean_phone_number(phone):
    """Remove spaces and non-digit characters from phone number."""
    if pd.isna(phone):
        return None
    # Convert to string and remove .0 if present (though CSV load might handle this)
    s = str(phone).replace('.0', '')
    # Remove non-digits
    cleaned = re.sub(r'\D', '', s)
    return cleaned

def get_trade_numbers(conn):
    """Fetch all phone numbers from clientlist table."""
    print("Fetching client list from database...")
    query = "SELECT account_telephone_number, sales_telephone_number, mobile_number FROM clientlist"
    df = pd.read_sql(query, conn)
    
    trade_numbers = set()
    for col in df.columns:
        # Clean and add non-empty numbers
        cleaned = df[col].apply(clean_phone_number)
        trade_numbers.update(cleaned.dropna().loc[cleaned != ''].tolist())
    
    # Remove '0' if it exists (common placeholder)
    trade_numbers.discard('0')
    print(f"Found {len(trade_numbers)} unique trade phone numbers.")
    return trade_numbers

def create_tables(conn):
    """Create call_logs and abandoned_calls tables."""
    cursor = conn.cursor()
    
    # Drop tables to ensure fresh schema
    cursor.execute("DROP TABLE IF EXISTS call_logs;")
    cursor.execute("DROP TABLE IF EXISTS abandoned_calls;")
    
    # Call Logs Table
    cursor.execute("""
        CREATE TABLE call_logs (
            call_id TEXT PRIMARY KEY,
            week INTEGER,
            call_start TIMESTAMP,
            from_number TEXT,
            to_number TEXT,
            direction TEXT,
            status TEXT,
            ringing_sec INTEGER,
            talking_sec INTEGER,
            customer_type TEXT,
            journey_details TEXT,
            is_answered BOOLEAN,
            is_abandoned BOOLEAN
        );
    """)
    
    # Abandoned Calls Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS abandoned_calls (
            id SERIAL PRIMARY KEY,
            week INTEGER,
            call_time TIMESTAMP,
            caller_id TEXT,
            customer_type TEXT,
            waiting_time TEXT,
            agent_state TEXT,
            polling_attempts INTEGER,
            queue TEXT
        );
    """)
    
    conn.commit()
    cursor.close()
    print("Tables created (if not existed).")

def ingest_call_logs(conn, trade_numbers):
    """Ingest cleaned call logs."""
    print("\nProcessing Call Logs...")
    try:
        df = pd.read_csv('reports/call_logs_cleaned.csv')
    except FileNotFoundError:
        print("reports/call_logs_cleaned.csv not found.")
        return

    # Clean from_number for matching
    df['clean_from'] = df['from_number'].apply(clean_phone_number)
    
    # Update customer type
    # If number is in trade_numbers OR already marked as trade (e.g. by name), set to trade
    # Otherwise retail
    def resolve_type(row):
        if row['clean_from'] in trade_numbers:
            return 'trade'
        if str(row['customer_type']).lower() == 'trade':
            return 'trade'
        return 'retail'
    
    df['final_customer_type'] = df.apply(resolve_type, axis=1)
    
    # Prepare for insertion
    # Map DataFrame columns to Table columns
    # Table: call_id, week, call_start, from_number, to_number, direction, status, ringing_sec, talking_sec, customer_type, journey_details, is_answered, is_abandoned
    
    insert_data = []
    for _, row in df.iterrows():
        week_val = row.get('week')
        week = int(week_val) if pd.notna(week_val) else 0
        
        ringing_val = row.get('ringing_total_sec')
        ringing = int(ringing_val) if pd.notna(ringing_val) else 0
        
        talking_val = row.get('talking_total_sec')
        talking = int(talking_val) if pd.notna(talking_val) else 0

        call_start = row.get('call_start')
        if pd.isna(call_start):
            call_start = None

        from_num = row.get('from_number')
        from_num = str(from_num) if pd.notna(from_num) else None

        to_num = row.get('to_number')
        to_num = str(to_num) if pd.notna(to_num) else None

        direction = row.get('directions')
        direction = str(direction) if pd.notna(direction) else None

        status = row.get('statuses')
        status = str(status) if pd.notna(status) else None

        journey = row.get('call_activity_details')
        journey = str(journey) if pd.notna(journey) else None

        insert_data.append((
            str(row['Call ID']),
            week,
            call_start,
            from_num,
            to_num,
            direction,
            status,
            ringing,
            talking,
            row['final_customer_type'],
            journey,
            bool(row['is_answered']),
            bool(row['is_abandoned'])
        ))
    
    cursor = conn.cursor()
    # Use upsert (ON CONFLICT DO UPDATE) to handle re-runs
    query = """
        INSERT INTO call_logs (
            call_id, week, call_start, from_number, to_number, direction, status, 
            ringing_sec, talking_sec, customer_type, journey_details, is_answered, is_abandoned
        ) VALUES %s
        ON CONFLICT (call_id) DO UPDATE SET
            week = EXCLUDED.week,
            customer_type = EXCLUDED.customer_type,
            journey_details = EXCLUDED.journey_details;
    """
    
    extras.execute_values(cursor, query, insert_data)
    conn.commit()
    cursor.close()
    print(f"Upserted {len(insert_data)} rows into call_logs.")

def ingest_abandoned_logs(conn, trade_numbers):
    """Ingest cleaned abandoned logs."""
    print("\nProcessing Abandoned Logs...")
    try:
        df = pd.read_csv('reports/abandoned_logs_cleaned.csv')
    except FileNotFoundError:
        print("reports/abandoned_logs_cleaned.csv not found.")
        return

    # Clean caller_id for matching
    df['clean_caller'] = df['Caller ID'].apply(clean_phone_number)
    
    # Update customer type
    def resolve_type(row):
        if row['clean_caller'] in trade_numbers:
            return 'trade'
        # Abandoned logs don't have names, so rely on phone match or existing logic
        if str(row['customer_type']).lower() == 'trade':
            return 'trade'
        return 'retail'
    
    df['final_customer_type'] = df.apply(resolve_type, axis=1)
    
    # Prepare for insertion
    # Table: week, call_time, caller_id, customer_type, waiting_time, agent_state, polling_attempts, queue
    
    insert_data = []
    for _, row in df.iterrows():
        week_val = row.get('week')
        week = int(week_val) if pd.notna(week_val) else 0
        
        polling_val = row.get('Polling Attempts')
        polling = int(polling_val) if pd.notna(polling_val) else 0
        
        call_time = row.get('Call Time')
        if pd.isna(call_time):
            call_time = None

        caller_id = row.get('Caller ID')
        caller_id = str(caller_id) if pd.notna(caller_id) else None

        waiting_time = row.get('Waiting Time')
        waiting_time = str(waiting_time) if pd.notna(waiting_time) else None

        agent_state = row.get('Agent State')
        agent_state = str(agent_state) if pd.notna(agent_state) else None

        queue = row.get('Queue')
        queue = str(queue) if pd.notna(queue) else None

        insert_data.append((
            week,
            call_time,
            caller_id,
            row['final_customer_type'],
            waiting_time,
            agent_state,
            polling,
            queue
        ))
    
    cursor = conn.cursor()
    # Truncate and replace for abandoned logs since we don't have a stable ID in the CSV (dedup happens in pandas)
    # Or we can just insert. Let's truncate to avoid duplicates on re-run.
    cursor.execute("TRUNCATE TABLE abandoned_calls;")
    
    query = """
        INSERT INTO abandoned_calls (
            week, call_time, caller_id, customer_type, waiting_time, agent_state, polling_attempts, queue
        ) VALUES %s
    """
    
    extras.execute_values(cursor, query, insert_data)
    conn.commit()
    cursor.close()
    print(f"Inserted {len(insert_data)} rows into abandoned_calls.")

def main():
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        # 1. Get Trade Numbers
        trade_numbers = get_trade_numbers(conn)
        
        # 2. Create Tables
        create_tables(conn)
        
        # 3. Ingest Data
        ingest_call_logs(conn, trade_numbers)
        ingest_abandoned_logs(conn, trade_numbers)
        
        print("\nData ingestion complete.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
