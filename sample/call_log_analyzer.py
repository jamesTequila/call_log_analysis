import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import execute_values
import os
from cleaning import run_cleaning
import glob
import re
import hashlib
from datetime import timedelta

# --- Anonymization Functions (Injected) ---
def anonymize_phone(phone):
    """Anonymize phone number by hashing."""
    if pd.isna(phone) or str(phone).strip() == '':
        return phone
    s = str(phone).strip()
    # Keep it looking like a phone number
    h = hashlib.md5(s.encode()).hexdigest()[:8]
    return f"+3538{h[:8]}"

def anonymize_name(name):
    """Anonymize customer name."""
    if pd.isna(name) or str(name).strip() == '':
        return name
    s = str(name).strip()
    if s.lower() == 'unknown': return s
    h = hashlib.md5(s.encode()).hexdigest()[:6]
    return f"CUSTOMER {h.upper()}"

def anonymize_text(text):
    """Anonymize sensitive info in text fields."""
    if pd.isna(text) or str(text).strip() == '':
        return text
    s = str(text)
    
    # 1. Anonymize Phone Numbers
    s = re.sub(r'(?:\+353|00353|353|0)\s?[\d\-\s]{8,}', '[PHONE]', s)
    s = re.sub(r'\((?:353|0)[\d\-\s]{7,}\)', '([PHONE])', s)
    
    # 2. Anonymize Agent Names / Persons
    s = re.sub(r'[A-Z][a-z]+,\s+[A-Z][a-z]+\s+\(\d+\)', '[AGENT]', s)
    s = re.sub(r'[A-Z][a-z]+\s+[A-Z][a-z]+\s+\(\d+\)', '[AGENT]', s)

    # Context-based replacement
    def repl_agent(match):
        name = match.group(1).strip()
        if 'Voice Agent' in name or 'System' in name or 'Queue' in name or 'Shared Parking' in name: return match.group(0)
        return match.group(0).replace(name, '[AGENT]')
        
    s = re.sub(r'Ended by ([^:|]+)', repl_agent, s)
    s = re.sub(r'Answered by ([^:|]+)', repl_agent, s)
    s = re.sub(r'was replaced by ([^:|]+)', repl_agent, s)
    s = re.sub(r'call was taken by ([^:|]+)', repl_agent, s)
    
    return s

def anonymize_dataframe(df, phone_cols=[], text_cols=[]):
    """Anonymize phone columns and text columns in a dataframe."""
    # Create copy to avoid SettingWithCopy warnings
    df = df.copy()
    
    for col in phone_cols:
        if col in df.columns:
            df[col] = df[col].apply(anonymize_phone)
            
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(anonymize_text)
            
    return df

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'kmc.tequila-ai.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'tequila_ai_reporting'),
    'user': os.getenv('DB_USER', 'james'),
    'password': os.getenv('DB_PASSWORD', ']dT1H-{ekquGfn^6'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

def save_to_database(df, table_name='call_logs'):
    """Save DataFrame to PostgreSQL database."""
    try:
        print(f"Connecting to database to save {len(df)} rows...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create table if not exists (simplified schema)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        # Generate CREATE TABLE statement based on DataFrame columns
        cols = []
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = 'NUMERIC'
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = 'TIMESTAMP'
            else:
                sql_type = 'TEXT'
            cols.append(f'"{col}" {sql_type}')
        
        create_sql = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {', '.join(cols)});"
        cursor.execute(create_sql)
        
        # Insert data
        columns = [f'"{col}"' for col in df.columns]
        values = [tuple(x) for x in df.to_numpy()]
        
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        execute_values(cursor, insert_sql, values)
        
        conn.commit()
        print(f"Successfully saved data to table '{table_name}'.")
        
    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def calculate_mode(series):
    """Calculate mode of a series, handling empty cases."""
    mode = series.mode()
    if not mode.empty:
        return mode.iloc[0]
    return 0

def get_week_date_label(week_num, max_date):
    """Generate week date label based on week number and max date.
    
    Week 1 = max_date going back 7 days
    Week 2 = 7 days before Week 1
    Week 3+ = older
    """
    if not isinstance(max_date, pd.Timestamp):
        max_date = pd.to_datetime(max_date)
    
    # Calculate based on max date going backwards
    # Week 1 = last 7 days from max_date
    # Week 2 = 7 days before that, etc.
    days_back = (week_num - 1) * 7
    week_end = max_date - pd.Timedelta(days=days_back)
    week_start = week_end - pd.Timedelta(days=6)  # 7 days total (inclusive)
    
    return week_start.strftime('%d/%m/%Y')

def generate_plots(df, abandoned_df):
    """Generate combined Plotly HTML with three subplots."""
    from plotly.subplots import make_subplots
    
    # Helper for time formatting
    def format_time_hover(seconds):
        if pd.isna(seconds):
            return "0s"
        if seconds < 60:
            return f"{int(seconds)}s"
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m {s}s"

    # --- Prepare Data for Charts 1 & 2 (Wait/Talk Time) ---
    # Filter for Week 1 and 2, and exclude 'unknown' customer type
    df_recent = df[df['week'].isin([1, 2])].copy()
    df_recent = df_recent[df_recent['customer_type'].isin(['retail', 'trade'])]
    
    # Normalize customer type labels for display
    df_recent['customer_type_display'] = df_recent['customer_type'].map({
        'retail': 'Retail',
        'trade': 'Trade Customer'
    })
    
    # Group by Week and Customer Type
    grouped = df_recent.groupby(['week', 'customer_type_display']).agg(
        avg_wait_time=('ringing_total_sec', 'mean'),
        avg_talk_time=('talking_total_sec', 'mean'),
        call_count=('ringing_total_sec', 'count')
    ).reset_index()
    
    # Define colors: This Week (blue), Last Week (orange)
    color_map = {1: '#2391DC', 2: '#DC6E23'}
    
    # Get max date for week labels
    max_date = df['call_start'].max() if 'call_start' in df.columns else pd.Timestamp.now()
    
    # Helper function to get user-friendly week labels
    # NOTE: In updated consistent logic, we just use Week 1 and Week 2 labels directly
    # But for display we want "This Week" (most recent) and "Last Week" (previous)
    # Our new logic: Week 1 is always the MOST RECENT 7 days ("This Week")
    # Week 2 is the PREVIOUS 7 days ("Last Week")
    def get_week_label_display(week):
        if week == 1:
            return "This Week"
        elif week == 2:
            return "Last Week"
        else:
            return f"Week {week}"
    
    # Chart 1: Average Wait Time
    fig_waiting = go.Figure()
    customer_types = ['Retail', 'Trade Customer']
    
    for week in [2, 1]:
        week_data = grouped[grouped['week'] == week]
        y_vals = []
        call_counts = []
        hover_texts = []
        
        for ctype in customer_types:
            row = week_data[week_data['customer_type_display'] == ctype]
            if len(row) > 0:
                val = row['avg_wait_time'].values[0]
                count = int(row['call_count'].values[0])
                y_vals.append(val) # Plot in seconds
                call_counts.append(count)
                hover_texts.append(format_time_hover(val))
            else:
                y_vals.append(0)
                call_counts.append(0)
                hover_texts.append("0s")
        
        week_label = get_week_label_display(week)
        
        fig_waiting.add_trace(go.Bar(
            name=week_label,
            x=customer_types,
            y=y_vals,
            marker_color=color_map[week],
            text=[week_label] * len(customer_types),  # Add label inside bar
            textposition='auto',
            customdata=np.array(list(zip(call_counts, hover_texts))),
            hovertemplate=(
                "Type: %{x}<br>"
                f"{week_label}<br>"
                "Avg Waiting Time: %{customdata[1]}<br>"
                "Total Calls: %{customdata[0]}<extra></extra>"
            )
        ))

    # Chart 2: Average Talk Time
    fig_talk = go.Figure()
    
    for week in [2, 1]:
        week_data = grouped[grouped['week'] == week]
        y_vals = []
        call_counts = []
        hover_texts = []
        
        for ctype in customer_types:
            row = week_data[week_data['customer_type_display'] == ctype]
            if len(row) > 0:
                val = row['avg_talk_time'].values[0]
                count = int(row['call_count'].values[0])
                y_vals.append(val / 60) # Plot in minutes
                call_counts.append(count)
                hover_texts.append(format_time_hover(val))
            else:
                y_vals.append(0)
                call_counts.append(0)
                hover_texts.append("0s")
        
        week_label = get_week_label_display(week)
        
        fig_talk.add_trace(go.Bar(
            name=week_label,
            x=customer_types,
            y=y_vals,
            marker_color=color_map[week],
            text=[week_label] * len(customer_types),  # Add label inside bar
            textposition='auto',
            customdata=np.array(list(zip(call_counts, hover_texts))),
            hovertemplate=(
                "Type: %{x}<br>"
                f"{week_label}<br>"
                "Avg Talk Time: %{customdata[1]}<br>"
                "Total Calls: %{customdata[0]}<extra></extra>"
            )
        ))

    # --- Prepare Data for Chart 3 (Abandoned by Day) ---
    fig_abandoned = go.Figure()
    day_counts = pd.DataFrame()
    
    if not abandoned_df.empty:
        # Process abandoned data
        abd = abandoned_df.copy()
        # Robust datetime parsing
        abd['Call Time'] = pd.to_datetime(abd['Call Time'], errors='coerce')
        
        # Filter weeks 1 & 2
        abd = abd[abd['week'].isin([1, 2])].copy()
        abd['day_of_week'] = abd['Call Time'].dt.day_name()
        
        # Parse Waiting Time to seconds for stats
        def parse_wait(x):
            try:
                if pd.isna(x): return 0
                parts = str(x).split(':')
                if len(parts) == 3: return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
                return 0
            except: return 0
        abd['wait_sec'] = abd['Waiting Time'].apply(parse_wait)
        
        # Prepare main df for total/answered stats
        main_df = df[df['week'].isin([1, 2])].copy()
        main_df['day_of_week'] = main_df['call_start'].dt.day_name()
        
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        for week in [2, 1]:  # Order week 2 (Last) then week 1 (This) for stacking or side-by-side
            # Using new consistent logic: Week 1 is "This Week", Week 2 is "Last Week"
            week_abd = abd[abd['week'] == week]
            week_main = main_df[main_df['week'] == week]
            
            y_vals = []
            custom_data = []
            
            for day in days_order:
                # Abandoned stats
                day_abd = week_abd[week_abd['day_of_week'] == day]
                abd_count = len(day_abd)
                y_vals.append(abd_count)
                
                # Wait time stats
                if not day_abd.empty:
                    min_w = day_abd['wait_sec'].min()
                    avg_w = day_abd['wait_sec'].mean()
                    max_w = day_abd['wait_sec'].max()
                else:
                    min_w = avg_w = max_w = 0
                
                # Main call stats
                day_main = week_main[week_main['day_of_week'] == day]
                
                # Count ALL main log calls (not just answered)
                main_calls_count = len(day_main)
                answered_calls = day_main['is_answered'].sum()
                
                # Total = All Main Log Calls + Abandoned
                total_calls = main_calls_count + abd_count
                
                # Voicemail/Other = Total Main - Answered
                voicemail_other = main_calls_count - int(answered_calls)
                
                custom_data.append([
                    total_calls,                # 0 - Main calls + Abandoned
                    int(answered_calls),        # 1 - Answered calls only
                    abd_count,                  # 2 - Abandoned
                    format_time_hover(min_w),   # 3
                    format_time_hover(avg_w),   # 4
                    format_time_hover(max_w),   # 5
                    voicemail_other             # 6 - Voicemail/Other
                ])
            
            week_label = get_week_label_display(week)
            all_calls_df = df # For the conditional check in the template string
            
            fig_abandoned.add_trace(go.Bar(
                name=week_label,
                x=days_order,
                y=y_vals,
                marker_color=color_map[week],
                customdata=custom_data,
                hovertemplate=(
                    "Day: %{x}<br>"
                    f"{week_label}<br>"
                    "Abandoned Calls: %{y}<br>"
                    + ("Answered Calls: %{customdata[1]}<br>" if all_calls_df is not None else "") +
                    ("Voicemail/Other: %{customdata[6]}<br>" if all_calls_df is not None else "") +
                    ("Total Calls: %{customdata[0]}<br>" if all_calls_df is not None else "") +
                    "Min Wait: %{customdata[3]}<br>"
                    "Average Wait: %{customdata[4]}<br>"
                    "Max Wait: %{customdata[5]}<extra></extra>"
                )
            ))
            
            # Store for annotation calculation
            if week == 1: week1_abd_data = week_abd
            if week == 2: week2_abd_data = week_abd
            if week == 1 or week == 2: day_counts = pd.concat([day_counts, week_abd])

    # --- Combine Plots ---
    combined_fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=(
            "Average Waiting Time",
            "Average Talking Time",
            "Abandoned Calls by Day of Week"
        ),
        specs=[[{}], [{}], [{}]],
        vertical_spacing=0.08,
        row_heights=[0.33, 0.33, 0.33]
    )
    
    for trace in fig_waiting.data:
        trace.showlegend = False
        combined_fig.add_trace(trace, row=1, col=1)
        
    for trace in fig_talk.data:
        trace.showlegend = False
        combined_fig.add_trace(trace, row=2, col=1)
        
    for trace in fig_abandoned.data:
        trace.showlegend = True # Show legend on bottom plot
        combined_fig.add_trace(trace, row=3, col=1)
        
    # --- Annotation ---
    if not abandoned_df.empty:
        # Recalculate global stats for annotation
        w1_mean = week1_abd_data['wait_sec'].mean() if 'week1_abd_data' in locals() and not week1_abd_data.empty else 0
        w2_mean = week2_abd_data['wait_sec'].mean() if 'week2_abd_data' in locals() and not week2_abd_data.empty else 0
        w1_count = len(week1_abd_data) if 'week1_abd_data' in locals() else 0
        w2_count = len(week2_abd_data) if 'week2_abd_data' in locals() else 0
        
        # Dynamic Y
        max_y = 10
        if not day_counts.empty:
             # Group by week/day to find max bar height
             cts = day_counts.groupby(['week', 'day_of_week']).size()
             if not cts.empty: max_y = cts.max()
        
        annotation_text = (
            f"<b>Avg Time to Abandon:</b><br>"
            f"Week {get_week_date_label(1, max_date)}: {format_time_hover(w1_mean)} ({w1_count} calls)<br>"
            f"Week {get_week_date_label(2, max_date)}: {format_time_hover(w2_mean)} ({w2_count} calls)"
        )
        
        combined_fig.add_annotation(
            text=annotation_text,
            xref="x3", yref="y3",
            x=-0.4, y=max_y * 0.8,
            showarrow=False,
            align="left",
            xanchor="left",
            yanchor="middle",
            bgcolor="rgba(255, 255, 255, 0.8)",
            bordercolor="black",
            borderwidth=1,
            font=dict(size=10)
        )

    # Update axis labels
    combined_fig.update_yaxes(title_text="Seconds", row=1, col=1)
    combined_fig.update_yaxes(title_text="Minutes", row=2, col=1)
    combined_fig.update_yaxes(title_text="Count", row=3, col=1)
    
    combined_fig.update_layout(
        title_text="Omni Consumer Products Call Center Metrics",
        title_x=0.5,
        showlegend=True,
        barmode="group",
        height=1200,
        autosize=True,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.05,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=20, r=20, t=60, b=50)
    )
    
    return {
        'combined_plot': combined_fig.to_html(
            full_html=False, 
            include_plotlyjs='cdn',
            config={'responsive': True, 'displayModeBar': False}
        )
    }

def load_abandoned_calls(data_dir='data'):
    """Load all abandoned calls CSV files."""
    files = glob.glob(os.path.join(data_dir, 'AbandonedCalls*.csv'))
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Clean Caller ID immediately to remove .0 suffix globally
        if 'Caller ID' in combined_df.columns:
            combined_df['Caller ID'] = combined_df['Caller ID'].astype(str).apply(
                lambda x: x[:-2] if x.endswith('.0') else x
            )
            
        # Deduplicate based on Caller ID and Call Time
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['Caller ID', 'Call Time'])
        print(f"Deduplicated abandoned logs: {before_dedup} -> {len(combined_df)}")
            
        # Save combined file
        output_path = os.path.join(data_dir, 'combined_abandoned_call_logs.csv')
        combined_df.to_csv(output_path, index=False)
        return combined_df
    return pd.DataFrame()

def analyze_abandoned_calls(abandoned_df, main_df):
    """Analyze abandoned calls with customer type mapping."""
    if abandoned_df.empty:
        return {}

    # Create mapping from Caller ID to Customer Type using main_df
    # Logic: Identify known Trade numbers. Everything else is Retail.
    
    # Get set of known trade numbers
    # Ensure main df numbers are strings and clean
    trade_numbers = set(main_df[main_df['customer_type'] == 'trade']['from_number'].astype(str).apply(lambda x: x.replace('.0', '')).unique())
    
    # Apply mapping: Check if Caller ID is in trade_numbers
    # Caller ID is already cleaned in load_abandoned_calls
    abandoned_df['customer_type'] = abandoned_df['Caller ID'].apply(
        lambda x: 'trade' if str(x) in trade_numbers else 'retail'
    )
    
    # USER REQUEST: Explicitly handle 'unknown' customer types -> retail
    if 'customer_type' in abandoned_df.columns:
        abandoned_df.loc[ abandoned_df['customer_type'].astype(str).str.lower() == 'unknown', 'customer_type'] = 'retail'
        # Also validation if needed: skip adding 'unknown' to specific trade lists (handled in main loop)
    
    # Calculate metrics
    metrics = {
        'total_abandoned_raw': len(abandoned_df),
        'retail_abandoned': len(abandoned_df[abandoned_df['customer_type'] == 'retail']),
        'trade_abandoned': len(abandoned_df[abandoned_df['customer_type'] == 'trade']),
        'unknown_abandoned': 0 
    }
    
    return metrics

def analyze_journey(main_df, abandoned_df):
    """Analyze caller journey including Queue, Voicemail, OOO, and Termination."""
    journey_stats = {}
    
    # --- 1. Main Log Analysis (Answered/Processed Calls) ---
    if 'call_activity_details' not in main_df.columns:
        main_df['call_activity_details'] = ''
        
    main_df['call_activity_details'] = main_df['call_activity_details'].fillna('')
    journey_df = main_df.copy()
    
    # Queue Usage
    journey_stats['queue_calls'] = len(journey_df[journey_df['call_activity_details'].str.contains('Queue', case=False)])
    
    # Out of Hours (Explicit OOO)
    journey_stats['ooo_calls'] = len(journey_df[journey_df['call_activity_details'].str.contains('Out of office', case=False)])
    
    # Voicemail (Voice Agent)
    journey_stats['voicemail_calls'] = len(journey_df[journey_df['call_activity_details'].str.contains('Voice Agent', case=False)])
    
    # Termination Analysis
    def extract_terminator(details):
        if 'Ended by Voice Agent' in details: return 'System'
        if 'Ended by' in details:
            m = re.search(r'Ended by ([^:|]+)', details)
            if m:
                terminator = m.group(1).strip()
                if any(char.isdigit() for char in terminator) and len(terminator) > 5:
                    return 'Customer'
                return 'Agent'
        return 'Unknown'
        
    journey_df['Terminator'] = journey_df['call_activity_details'].apply(extract_terminator)
    term_counts = journey_df['Terminator'].value_counts()
    journey_stats['ended_by_agent'] = term_counts.get('Agent', 0)
    journey_stats['ended_by_customer'] = term_counts.get('Customer', 0)
    journey_stats['ended_by_system'] = term_counts.get('System', 0)
    
    # --- 2. Abandoned Log Analysis (Abandoned Journey) ---
    if not abandoned_df.empty:
        if 'Agent State' in abandoned_df.columns:
            logged_out_df = abandoned_df[abandoned_df['Agent State'] == 'Logged Out'].copy()
            journey_stats['abd_agent_logged_out'] = len(logged_out_df)
            journey_stats['abd_agent_logged_in'] = len(abandoned_df[abandoned_df['Agent State'] == 'Logged In'])
            
            # Time breakdown for agents logged out calls
            if not logged_out_df.empty and 'Call Time' in logged_out_df.columns:
                logged_out_df['Call Time'] = pd.to_datetime(logged_out_df['Call Time'], errors='coerce')
                logged_out_df['hour'] = logged_out_df['Call Time'].dt.hour
                logged_out_df['day_of_week'] = logged_out_df['Call Time'].dt.dayofweek
                
                # Count calls before and after operating hours
                before_hours = 0
                after_hours = 0
                during_hours = 0
                
                for idx, row in logged_out_df.iterrows():
                    day = row['day_of_week']
                    hour = row['hour']
                    
                    if pd.isna(day) or pd.isna(hour):
                        continue
                        
                    # Operating hours check (Simplistic)
                    if day <= 4:  # Monday-Friday
                        if hour < 8: before_hours += 1
                        elif hour >= 20: after_hours += 1
                        else: during_hours += 1
                    elif day == 5:  # Saturday
                        if hour < 8: before_hours += 1
                        elif hour >= 18: after_hours += 1
                        else: during_hours += 1
                    elif day == 6:  # Sunday
                        if hour < 10: before_hours += 1
                        elif hour >= 16: after_hours += 1
                        else: during_hours += 1
                
                journey_stats['abd_logged_out_before_hours'] = before_hours
                journey_stats['abd_logged_out_after_hours'] = after_hours
                journey_stats['abd_logged_out_during_hours'] = during_hours
            else:
                journey_stats['abd_logged_out_before_hours'] = 0
                journey_stats['abd_logged_out_after_hours'] = 0
                journey_stats['abd_logged_out_during_hours'] = 0
        else:
            journey_stats['abd_agent_logged_out'] = 0
            journey_stats['abd_agent_logged_in'] = 0
            journey_stats['abd_logged_out_before_hours'] = 0
            journey_stats['abd_logged_out_after_hours'] = 0
            journey_stats['abd_logged_out_during_hours'] = 0
            
        if 'Polling Attempts' in abandoned_df.columns:
            journey_stats['abd_zero_polling'] = len(abandoned_df[abandoned_df['Polling Attempts'] == 0])
        else:
            journey_stats['abd_zero_polling'] = 0
    else:
        journey_stats['abd_agent_logged_out'] = 0
        journey_stats['abd_agent_logged_in'] = 0
        journey_stats['abd_zero_polling'] = 0
        
    return journey_stats

def analyze_calls(data_dir='data'):
    """Main analysis function. Loads all CallLog files in data_dir."""
    # 1. Clean and Load Data (Multiple Files)
    print("Cleaning and loading main call logs...")
    files = glob.glob(os.path.join(data_dir, 'CallLogLastWeek_*.csv'))
    dfs = []
    for f in files:
        print(f"Processing {f}...")
        try:
            cleaned = run_cleaning(f)
            dfs.append(cleaned.call_level_df)
        except Exception as e:
            print(f"Error processing {f}: {e}")
            
    if not dfs:
        print("No main call logs found!")
        return {}
        
    df = pd.concat(dfs, ignore_index=True)
    # Deduplicate Main Log (in case of file overlap)
    df = df.drop_duplicates(subset=['Call ID'])
    print(f"Total Main Log Calls (Unique): {len(df)}")

    # DATE-BASED WEEK CALCULATION (Consistent Logic)
    max_date = df['call_start'].max()
    print(f"Global Max Date: {max_date}")
    
    # This Week (Week 1): Most recent 7 days (inclusive of max_date)
    # E.g. Dec 14 is max. Week 1 = Dec 8 - Dec 14
    this_week_end = max_date
    this_week_start = max_date - timedelta(days=6)
    
    # Last Week (Week 2): Previous 7 days, NO OVERLAP
    # E.g. Week 2 = Dec 1 - Dec 7
    last_week_end = this_week_start - timedelta(days=1)
    last_week_start = last_week_end - timedelta(days=6)
    
    print(f"This Week (Week 1): {this_week_start.date()} to {this_week_end.date()}")
    print(f"Last Week (Week 2): {last_week_start.date()} to {last_week_end.date()}")
    
    def assign_week_global(dt):
        if dt >= this_week_start and dt <= this_week_end:
            return 1
        elif dt >= last_week_start and dt <= last_week_end:
            return 2
        else:
            return 3
            
    df['week'] = df['call_start'].apply(assign_week_global)
    print(f"Week distribution: {df['week'].value_counts().to_dict()}")

    # 3. Analyze Abandoned Calls
    print("Analyzing abandoned calls files...")
    abandoned_df = load_abandoned_calls(data_dir)
    
    # 3a. Assign customer type to abandoned_df BEFORE calculating weekly metrics
    if not abandoned_df.empty:
        # Helper to clean phone numbers for matching
        def clean_phone_for_match(phone):
            s = str(phone).strip()
            # Remove formatting chars first
            s = s.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
            if s.startswith('+353'): s = s[4:]
            elif s.startswith('00353'): s = s[5:]
            elif s.startswith('353'): s = s[3:]
            if s.startswith('0'): s = s[1:]
            return s

        # Get set of known trade numbers from main log (cleaned)
        trade_numbers = set(
            df[df['customer_type'] == 'trade']['from_number']
            .apply(clean_phone_for_match)
            .unique()
        )
        if 'anonymous' in trade_numbers: trade_numbers.remove('anonymous')
        if '' in trade_numbers: trade_numbers.remove('')
        
        # Apply mapping
        abandoned_df['customer_type'] = abandoned_df['Caller ID'].apply(
            lambda x: 'trade' if clean_phone_for_match(x) in trade_numbers else 'retail'
        )
        
        # USER REQUEST: Explicit cleanup of 'unknown'
        if 'customer_type' in abandoned_df.columns:
             abandoned_df.loc[ abandoned_df['customer_type'].astype(str).str.lower() == 'unknown', 'customer_type'] = 'retail'

        # Week assignment for abandoned calls
        abandoned_df['Call Time'] = pd.to_datetime(abandoned_df['Call Time'], errors='coerce')
        abandoned_df['week'] = abandoned_df['Call Time'].apply(assign_week_global)

    # 4. Generate PLOTS (Pass Week 1 & 2 DataFrames)
    # The generate_plots function internally filters for Week 1 & 2
    plot_results = generate_plots(df, abandoned_df)

    # 5. Calculate Scalar Metrics for Executive Summary (BOTTOM UP APPROACH)
    # We use the same filtered datasets as the plots to ensure numbers match exactly.
    
    metrics = {}
    
    # Helpers to get weekly slices
    def get_weekly_data(week_num):
        w_main = df[df['week'] == week_num]
        w_abd = abandoned_df[abandoned_df['week'] == week_num] if not abandoned_df.empty else pd.DataFrame()
        return w_main, w_abd
    
    # --- THIS WEEK (Week 1) ---
    week1_main, week1_abd = get_weekly_data(1)
    
    # Main log breakdown
    week1_retail_main = len(week1_main[week1_main['customer_type'] == 'retail'])
    week1_trade_main = len(week1_main[week1_main['customer_type'] == 'trade'])
    
    # Abandoned breakdown
    week1_retail_abd = len(week1_abd[week1_abd['customer_type'] == 'retail']) if not week1_abd.empty else 0
    week1_trade_abd = len(week1_abd[week1_abd['customer_type'] == 'trade']) if not week1_abd.empty else 0
    
    # Volume Metrics
    metrics['week1_retail_calls'] = week1_retail_main + week1_retail_abd
    metrics['week1_trade_calls'] = week1_trade_main + week1_trade_abd
    metrics['week1_calls'] = len(week1_main) + len(week1_abd)
    
    # Abandonment Metrics
    metrics['week1_retail_abandoned'] = week1_retail_abd
    metrics['week1_trade_abandoned'] = week1_trade_abd
    metrics['week1_abandoned_calls'] = len(week1_abd)
    
    # Abandonment Rates
    metrics['week1_retail_abandonment_rate'] = round((week1_retail_abd / metrics['week1_retail_calls'] * 100), 1) if metrics['week1_retail_calls'] > 0 else 0
    metrics['week1_trade_abandonment_rate'] = round((week1_trade_abd / metrics['week1_trade_calls'] * 100), 1) if metrics['week1_trade_calls'] > 0 else 0
    
    # Capture Dates for Verification
    metrics['this_week_start'] = this_week_start.strftime('%Y-%m-%d')
    metrics['this_week_end'] = this_week_end.strftime('%Y-%m-%d')
    
    # --- LAST WEEK (Week 2) ---
    week2_main, week2_abd = get_weekly_data(2)
    
    # Main log breakdown
    week2_retail_main = len(week2_main[week2_main['customer_type'] == 'retail'])
    week2_trade_main = len(week2_main[week2_main['customer_type'] == 'trade'])
    
    # Abandoned breakdown
    week2_retail_abd = len(week2_abd[week2_abd['customer_type'] == 'retail']) if not week2_abd.empty else 0
    week2_trade_abd = len(week2_abd[week2_abd['customer_type'] == 'trade']) if not week2_abd.empty else 0
    
    # Volume Metrics
    metrics['week2_retail_calls'] = week2_retail_main + week2_retail_abd
    metrics['week2_trade_calls'] = week2_trade_main + week2_trade_abd
    metrics['week2_calls'] = len(week2_main) + len(week2_abd)
    
    # Abandonment Metrics
    metrics['week2_retail_abandoned'] = week2_retail_abd
    metrics['week2_trade_abandoned'] = week2_trade_abd
    metrics['week2_abandoned_calls'] = len(week2_abd)
    
    # Abandonment Rates
    metrics['week2_retail_abandonment_rate'] = round((week2_retail_abd / metrics['week2_retail_calls'] * 100), 1) if metrics['week2_retail_calls'] > 0 else 0
    metrics['week2_trade_abandonment_rate'] = round((week2_trade_abd / metrics['week2_trade_calls'] * 100), 1) if metrics['week2_trade_calls'] > 0 else 0

    metrics['last_week_start'] = last_week_start.strftime('%Y-%m-%d')
    metrics['last_week_end'] = last_week_end.strftime('%Y-%m-%d')
    
    # Combined Totals
    metrics['total_calls'] = metrics['week1_calls'] + metrics['week2_calls']

    # Journey Stats
    journey_stats = analyze_journey(df[df['week'] == 1], abandoned_df[abandoned_df['week'] == 1] if not abandoned_df.empty else pd.DataFrame())
    metrics.update(journey_stats)
    
    # OOH Stats
    ooh_stats = {'ooh_total': 0, 'ooh_before_opening': 0, 'ooh_after_closing': 0} # Placeholder if not implemented fully
    metrics.update(ooh_stats)

    # Narrative Generation
    # Calculate percentage changes
    def calc_pct(curr, prev):
        if prev == 0: return 100.0 if curr > 0 else 0.0
        return round(((curr - prev) / prev) * 100, 1)

    retail_change = calc_pct(metrics['week1_retail_calls'], metrics['week2_retail_calls'])
    trade_change = calc_pct(metrics['week1_trade_calls'], metrics['week2_trade_calls'])
    abd_change = calc_pct(metrics['week1_abandoned_calls'], metrics['week2_abandoned_calls'])
    total_change = calc_pct(metrics['week1_calls'], metrics['week2_calls'])

    narrative = {
        'total_calls_text': f"{metrics['week1_calls']:,} calls ({'+' if total_change >= 0 else ''}{total_change}%)",
        'retail_text': f"Retail: {metrics['week1_retail_calls']:,} ({'+' if retail_change >= 0 else ''}{retail_change}%)",
        'trade_text': f"Trade: {metrics['week1_trade_calls']:,} ({'+' if trade_change >= 0 else ''}{trade_change}%)",
        'abandoned_text': f"Abandoned: {metrics['week1_abandoned_calls']:,} ({'+' if abd_change >= 0 else ''}{abd_change}%)"
    }
    
    # --- Prepare Abandoned Trade List (This Week Only) ---
    abd_trade_list = []
    if not abandoned_df.empty:
        # Filter for this week (Week 1) AND Trade customers
        abd_trade_df = abandoned_df[
            (abandoned_df['week'] == 1) & 
            (abandoned_df['customer_type'] == 'trade')
        ].copy()
        
        # USER REQUEST: Do not include 'Unknown' names
        # Assuming you have a way to loop up names, but here we just list numbers/times
        # If names are available in main log, we could merge. 
        # For now, we list what we have.
        
        # Sort by latest first
        abd_trade_df = abd_trade_df.sort_values('Call Time', ascending=False)
        
        for _, row in abd_trade_df.iterrows():
            item = {
                'time': row['Call Time'].strftime('%H:%M %d/%m') if not pd.isna(row['Call Time']) else 'N/A',
                'number': row['Caller ID'],
                'wait': row['Waiting Time']
            }
            # Check for unknown name if we had name data (we don't here, only number)
            # But we can skip if number is 'anonymous' or empty
            if item['number'] and str(item['number']).lower() not in ['anonymous', 'unknown', 'none', '']:
                 abd_trade_list.append(item)

    # --- ANONYMIZATION STEP (NEW) ---
    # Apply anonymization to dataframes BEFORE returning raw data tables
    # Phone columns: 'from_number', 'to_number', 'Caller ID'
    # Text columns: 'call_activity_details'
    
    print("Anonymizing data for report...")
    df_clean = anonymize_dataframe(df, phone_cols=['from_number', 'to_number'], text_cols=['call_activity_details'])
    abd_clean = anonymize_dataframe(abandoned_df, phone_cols=['Caller ID'])
    
    # Also anonymize the abd_trade_list numbers
    for item in abd_trade_list:
        item['number'] = anonymize_phone(item['number'])

    return {
        'metrics': metrics,
        'plots': plot_results['combined_plot'],
        'narrative': narrative,
        'raw_data': df_clean[df_clean['week'] == 1].head(10).to_dict('records'), # Sample for table
        'abandoned_logs': abd_clean[abd_clean['week'] == 1].head(10).to_dict('records'),
        'max_date': max_date.strftime('%d/%m/%Y'),
        'abandoned_trade_customers': {'week1': abd_trade_list}
    }

if __name__ == "__main__":
    analyze_calls('data')
