"""
Historical Report Snapshot Storage
Stores metrics from each report run to enable historical comparisons.
"""
import psycopg2
from datetime import datetime, date
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration - credentials from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'kmc.tequila-ai.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'tequila_ai_reporting'),
    'user': os.getenv('DB_USER', 'james'),
    'password': os.getenv('DB_PASSWORD'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_snapshot_table():
    """Create report_snapshots table if it doesn't exist."""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_snapshots (
                id SERIAL PRIMARY KEY,
                report_date DATE NOT NULL,
                week_number INTEGER NOT NULL,
                week_label TEXT,
                week_start_date DATE,
                week_end_date DATE,
                total_calls INTEGER,
                retail_calls INTEGER,
                trade_calls INTEGER,
                abandoned_calls INTEGER,
                answered_calls INTEGER,
                abandonment_rate DECIMAL(5,2),
                retail_abandonment_rate DECIMAL(5,2),
                trade_abandonment_rate DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_date, week_number)
            );
        """)
        conn.commit()
        cursor.close()
        print("Report snapshots table ready.")
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        conn.close()

def create_weekly_metrics_table():
    """Create weekly_metrics table keyed on week date range for persistent storage."""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weekly_metrics (
                id SERIAL,
                week_start_date DATE NOT NULL,
                week_end_date DATE NOT NULL,
                report_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_calls INTEGER,
                retail_calls INTEGER,
                trade_calls INTEGER,
                abandoned_calls INTEGER,
                abandoned_retail INTEGER,
                abandoned_trade INTEGER,
                abandonment_rate DECIMAL(5,2),
                retail_abandonment_rate DECIMAL(5,2),
                trade_abandonment_rate DECIMAL(5,2),
                PRIMARY KEY (week_start_date, week_end_date)
            );
        """)
        conn.commit()
        cursor.close()
        print("Weekly metrics table ready.")
        return True
    except Exception as e:
        print(f"Error creating weekly_metrics table: {e}")
        return False
    finally:
        conn.close()

def store_weekly_metrics(metrics):
    """Store processed weekly metrics with UPSERT keyed on week date range.

    Stores one row per week (This Week and Last Week) so the same calendar
    week always maps to the same row regardless of when the report is run.
    """
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Store This Week (week 1)
        cursor.execute("""
            INSERT INTO weekly_metrics (
                week_start_date, week_end_date,
                total_calls, retail_calls, trade_calls,
                abandoned_calls, abandoned_retail, abandoned_trade,
                abandonment_rate, retail_abandonment_rate, trade_abandonment_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (week_start_date, week_end_date) DO UPDATE SET
                total_calls = EXCLUDED.total_calls,
                retail_calls = EXCLUDED.retail_calls,
                trade_calls = EXCLUDED.trade_calls,
                abandoned_calls = EXCLUDED.abandoned_calls,
                abandoned_retail = EXCLUDED.abandoned_retail,
                abandoned_trade = EXCLUDED.abandoned_trade,
                abandonment_rate = EXCLUDED.abandonment_rate,
                retail_abandonment_rate = EXCLUDED.retail_abandonment_rate,
                trade_abandonment_rate = EXCLUDED.trade_abandonment_rate,
                report_generated_at = CURRENT_TIMESTAMP
        """, (
            metrics['this_week_start'],
            metrics['this_week_end'],
            metrics['week1_calls'],
            metrics['week1_retail_total'],
            metrics['week1_trade_total'],
            metrics.get('week1_retail_abandoned', 0) + metrics.get('week1_trade_abandoned', 0),
            metrics.get('week1_retail_abandoned', 0),
            metrics.get('week1_trade_abandoned', 0),
            metrics.get('week1_retail_abandonment_rate', 0) + metrics.get('week1_trade_abandonment_rate', 0),
            metrics.get('week1_retail_abandonment_rate', 0),
            metrics.get('week1_trade_abandonment_rate', 0)
        ))

        # Store Last Week (week 2)
        cursor.execute("""
            INSERT INTO weekly_metrics (
                week_start_date, week_end_date,
                total_calls, retail_calls, trade_calls,
                abandoned_calls, abandoned_retail, abandoned_trade,
                abandonment_rate, retail_abandonment_rate, trade_abandonment_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (week_start_date, week_end_date) DO UPDATE SET
                total_calls = EXCLUDED.total_calls,
                retail_calls = EXCLUDED.retail_calls,
                trade_calls = EXCLUDED.trade_calls,
                abandoned_calls = EXCLUDED.abandoned_calls,
                abandoned_retail = EXCLUDED.abandoned_retail,
                abandoned_trade = EXCLUDED.abandoned_trade,
                abandonment_rate = EXCLUDED.abandonment_rate,
                retail_abandonment_rate = EXCLUDED.retail_abandonment_rate,
                trade_abandonment_rate = EXCLUDED.trade_abandonment_rate,
                report_generated_at = CURRENT_TIMESTAMP
        """, (
            metrics['last_week_start'],
            metrics['last_week_end'],
            metrics['week2_calls'],
            metrics['week2_retail_total'],
            metrics['week2_trade_total'],
            metrics.get('week2_retail_abandoned', 0) + metrics.get('week2_trade_abandoned', 0),
            metrics.get('week2_retail_abandoned', 0),
            metrics.get('week2_trade_abandoned', 0),
            metrics.get('week2_retail_abandonment_rate', 0) + metrics.get('week2_trade_abandonment_rate', 0),
            metrics.get('week2_retail_abandonment_rate', 0),
            metrics.get('week2_trade_abandonment_rate', 0)
        ))

        conn.commit()
        cursor.close()
        print(f"Stored weekly metrics for {metrics['this_week_start']} to {metrics['this_week_end']} and {metrics['last_week_start']} to {metrics['last_week_end']}")
        return True

    except Exception as e:
        print(f"Error storing weekly metrics: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        conn.close()

def store_snapshot(metrics, report_date=None):
    """Store metrics snapshot for historical comparison.

    Now also populates week_start_date and week_end_date columns.
    """
    if report_date is None:
        report_date = date.today()

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Store Week 1 (This Week) - now includes week date range
        cursor.execute("""
            INSERT INTO report_snapshots (
                report_date, week_number, week_label,
                week_start_date, week_end_date,
                total_calls, retail_calls, trade_calls,
                abandoned_calls, abandonment_rate,
                retail_abandonment_rate, trade_abandonment_rate
            ) VALUES (%s, 1, 'This Week', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, week_number) DO UPDATE SET
                week_start_date = EXCLUDED.week_start_date,
                week_end_date = EXCLUDED.week_end_date,
                total_calls = EXCLUDED.total_calls,
                retail_calls = EXCLUDED.retail_calls,
                trade_calls = EXCLUDED.trade_calls,
                abandoned_calls = EXCLUDED.abandoned_calls,
                abandonment_rate = EXCLUDED.abandonment_rate,
                retail_abandonment_rate = EXCLUDED.retail_abandonment_rate,
                trade_abandonment_rate = EXCLUDED.trade_abandonment_rate
        """, (
            report_date,
            metrics['this_week_start'],
            metrics['this_week_end'],
            metrics['week1_calls'],
            metrics['week1_retail_total'],
            metrics['week1_trade_total'],
            metrics['week1_retail_abandoned'] + metrics['week1_trade_abandoned'],
            metrics['abandonment_rate'],
            metrics['week1_retail_abandonment_rate'],
            metrics['week1_trade_abandonment_rate']
        ))

        # Store Week 2 (Last Week) - now includes week date range
        cursor.execute("""
            INSERT INTO report_snapshots (
                report_date, week_number, week_label,
                week_start_date, week_end_date,
                total_calls, retail_calls, trade_calls,
                abandoned_calls, abandonment_rate,
                retail_abandonment_rate, trade_abandonment_rate
            ) VALUES (%s, 2, 'Last Week', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, week_number) DO UPDATE SET
                week_start_date = EXCLUDED.week_start_date,
                week_end_date = EXCLUDED.week_end_date,
                total_calls = EXCLUDED.total_calls,
                retail_calls = EXCLUDED.retail_calls,
                trade_calls = EXCLUDED.trade_calls,
                abandoned_calls = EXCLUDED.abandoned_calls,
                abandonment_rate = EXCLUDED.abandonment_rate,
                retail_abandonment_rate = EXCLUDED.retail_abandonment_rate,
                trade_abandonment_rate = EXCLUDED.trade_abandonment_rate
        """, (
            report_date,
            metrics['last_week_start'],
            metrics['last_week_end'],
            metrics['week2_calls'],
            metrics['week2_retail_total'],
            metrics['week2_trade_total'],
            metrics['week2_retail_abandoned'] + metrics['week2_trade_abandoned'],
            metrics['abandonment_rate'],
            metrics['week2_retail_abandonment_rate'],
            metrics['week2_trade_abandonment_rate']
        ))

        conn.commit()
        cursor.close()
        print(f"Stored snapshot for {report_date}")
        return True

    except Exception as e:
        print(f"Error storing snapshot: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        conn.close()

def get_previous_report_comparison():
    """Get the previous report's 'This Week' to compare with current 'Last Week'."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        query = """
            SELECT report_date, week_start_date, week_end_date,
                   total_calls, retail_calls, trade_calls,
                   abandoned_calls, abandonment_rate
            FROM report_snapshots
            WHERE week_label = 'This Week'
            ORDER BY report_date DESC
            LIMIT 1 OFFSET 1
        """
        df = pd.read_sql(query, conn)
        return df if not df.empty else None
    except Exception as e:
        print(f"Error fetching previous report: {e}")
        return None
    finally:
        conn.close()

def get_weekly_metrics_history(limit=10):
    """Get historical weekly metrics for trend analysis."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        query = """
            SELECT week_start_date, week_end_date,
                   total_calls, retail_calls, trade_calls,
                   abandoned_calls, abandoned_retail, abandoned_trade,
                   abandonment_rate, retail_abandonment_rate, trade_abandonment_rate,
                   report_generated_at
            FROM weekly_metrics
            ORDER BY week_start_date DESC
            LIMIT %s
        """
        df = pd.read_sql(query, conn, params=(limit,))
        return df if not df.empty else None
    except Exception as e:
        print(f"Error fetching weekly metrics history: {e}")
        return None
    finally:
        conn.close()

if __name__ == "__main__":
    # Test table creation
    create_snapshot_table()
    create_weekly_metrics_table()
    print("Tables created successfully!")
