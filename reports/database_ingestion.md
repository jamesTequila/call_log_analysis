# Database Ingestion Documentation

## Overview
This document describes the database schema and ingestion process for the Southside Call Analysis project. The system ingests processed call logs into a PostgreSQL database, enriching the data with customer type information derived from the `clientlist` table.

## Database Schema

### 1. `call_logs` Table
Stores the cleaned and aggregated main call logs.

| Column | Type | Description |
|--------|------|-------------|
| `call_id` | TEXT (PK) | Unique identifier for the call |
| `week` | INTEGER | Week number (1 or 2) |
| `call_start` | TIMESTAMP | Start time of the call |
| `from_number` | TEXT | Caller's phone number |
| `to_number` | TEXT | Dialed number |
| `direction` | TEXT | Call direction(s) (e.g., Inbound, Queue) |
| `status` | TEXT | Call status(es) (e.g., Answered, Missed) |
| `ringing_sec` | INTEGER | Total ringing duration in seconds |
| `talking_sec` | INTEGER | Total talking duration in seconds |
| `customer_type` | TEXT | 'Trade' or 'Retail' (enriched) |
| `journey_details` | TEXT | Detailed call activity log |
| `is_answered` | BOOLEAN | Whether the call was answered |
| `is_abandoned` | BOOLEAN | Whether the call was abandoned |

### 2. `abandoned_calls` Table
Stores the cleaned abandoned call logs.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL (PK) | Auto-incrementing ID |
| `week` | INTEGER | Week number (1 or 2) |
| `call_time` | TIMESTAMP | Time of the call |
| `caller_id` | TEXT | Caller's phone number |
| `customer_type` | TEXT | 'Trade' or 'Retail' (enriched) |
| `waiting_time` | TEXT | Duration caller waited before abandoning |
| `agent_state` | TEXT | Agent status (e.g., Logged In, Logged Out) |
| `polling_attempts` | INTEGER | Number of polling attempts |
| `queue` | TEXT | Queue name |

## Ingestion Process

The ingestion script `ingest_data.py` performs the following steps:

1.  **Connect to Database**: Uses credentials defined in the script (matching `reference/database_connection_test.ipynb`).
2.  **Fetch Trade Numbers**: Queries the `clientlist` table to get all `account_telephone_number`, `sales_telephone_number`, and `mobile_number` values. These are cleaned (spaces/non-digits removed) to create a set of known Trade numbers.
3.  **Create Tables**: Drops and recreates `call_logs` and `abandoned_calls` tables to ensure a fresh schema.
4.  **Ingest Main Call Logs**:
    *   Reads `reports/call_logs_cleaned.csv`.
    *   **Enrichment**: Checks if `from_number` exists in the Trade numbers set. If yes, sets `customer_type` to 'Trade'. Otherwise, defaults to 'Retail' (unless already identified as Trade by name).
    *   Inserts data into `call_logs` using upsert (on conflict update).
5.  **Ingest Abandoned Logs**:
    *   Reads `reports/abandoned_logs_cleaned.csv`.
    *   **Enrichment**: Checks if `Caller ID` exists in the Trade numbers set. If yes, sets `customer_type` to 'Trade'.
    *   Inserts data into `abandoned_calls`.

## Usage

To run the ingestion process:

```bash
python ingest_data.py
```

## Dependencies
*   `pandas`
*   `psycopg2`
*   `python-dotenv` (optional, if using .env for credentials)
