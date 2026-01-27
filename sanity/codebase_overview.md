# Codebase Overview

This document provides a reference for the Python scripts used in the Southside Call Analysis project.

## Core Scripts

### `call_log_analyzer.py`
**Purpose**: The main orchestration script. It loads data, runs analysis, calculates metrics, and generates the final HTML report.
**Key Functions**:
- `analyze_calls(data_dir)`: Main entry point. Loads logs, calculates weeks, runs sub-analyses.
- `save_to_database(df)`: Saves processed logs to PostgreSQL.
- `generate_plots(df, abandoned_df)`: Creates Plotly visualizations.
- `load_abandoned_calls()`: Reads and combines abandoned call logs.
- `analyze_journey(main_df, abandoned_df)`: Calculates journey metrics (Queue, Voicemail, Termination).

### `cleaning.py`
**Purpose**: Handles raw data cleaning and aggregation.
**Key Functions**:
- `run_cleaning(path)`: Main cleaning pipeline for a single file.
- `clean_call_log(path)`: Parses dates, converts durations to seconds.
- `aggregate_to_call_level(df)`: Groups leg-level data into single call records.
- `classify_customer_from_activity(activity)`: Determines Trade vs. Retail based on activity strings.

### `call_analytics_utils.py`
**Purpose**: Shared utility functions for date handling and visualization.
**Key Functions**:
- `get_week_date_label(week_num, max_date)`: Returns the "Week starting" date string.
- `hms_to_seconds(val)`: robust time string conversion.
- `extract_phone_number(caller_id)`: Extracts digits from various caller ID formats.

### `ooh_analysis.py`
**Purpose**: Specialized logic for Out of Hours (OOH) analysis.
**Key Functions**:
- `analyze_out_of_hours(main_df, abandoned_df)`: Categorizes calls as "during", "before opening", or "after closing" based on specific operating hours (Mon-Fri 8-8, Sat 8-6, Sun 10-4).

### `generate_report.py`
**Purpose**: Entry point for running the full report generation process.
**Key Functions**:
- `generate_report()`: Calls `analyze_calls` and then renders the Jinja2 template.

### `ingest_data.py`
**Purpose**: Database ingestion script (separate from report generation).
**Key Functions**:
- `ingest_call_logs(conn)`: Loads call logs into the database.
- `get_trade_numbers(conn)`: Fetches trade numbers from the database (if available).

## Sanity Check Suite (`sanity/`)

### `sanity/verify_data_integrity.py`
**Purpose**: Checks for data loss or corruption.
**Key Functions**:
- `verify_integrity()`: Compares raw row counts vs. processed row counts, checks for duplicates.

### `sanity/verify_metrics.py`
**Purpose**: Independent verification of dashboard numbers.
**Key Functions**:
- `verify_metrics()`: Re-calculates totals and breakdowns using simple pandas logic and generates `dashboard_comparison.md`.

### `sanity/test_core_logic.py`
**Purpose**: Unit tests for critical business logic.
**Key Functions**:
- `test_logic()`: Tests phone number cleaning and week assignment with specific test cases.

### `sanity/generate_sample_audit.py`
**Purpose**: Creates a manual audit file.
**Key Functions**:
- `generate_audit()`: Exports a random sample of 50 calls to `audit_sample.csv`.
