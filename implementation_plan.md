# Implementation Plan - Call Log Analysis Pipeline

**Created:** 2026-01-27
**Status:** In Progress

---

## Phase 1: Fix Week Logic (Priority: HIGH)

### Problem
Three interacting bugs in `call_log_analyzer.py` cause week-to-week numbers to be inconsistent across consecutive reports. The same calendar week shows different totals depending on whether it appears as "This Week" or "Last Week".

### Root Cause
1. `generate_plots()` swaps week labels - Week 2 (older data) displayed as "This Week", Week 1 (newest) as "Last Week"
2. Abandoned data is cross-wired to the wrong week in the abandoned chart
3. `metrics.update(plot_metrics)` overwrites correctly-calculated date-based metrics with the swapped plot values

### Fix
- [x] **1a.** Fix `get_week_label_display()` - Week 1 = "This Week", Week 2 = "Last Week"
- [x] **1b.** Fix `color_map` - blue = This Week (week 1), orange = Last Week (week 2)
- [x] **1c.** Fix loop iteration order - `for week in [1, 2]` so "This Week" renders first
- [x] **1d.** Fix `metric_week` mapping - use `f"week{week}"` instead of swapped mapping
- [x] **1e.** Remove abandoned data cross-swap - use `abd[abd['week'] == week]` directly
- [x] **1f.** Remove `metrics.update(plot_metrics)` override - let `analyze_calls()` date-based metrics be the single source of truth

---

## Phase 2: Persistent Database Storage (Priority: HIGH)

### Problem
- `save_to_database()` DROP and recreates `call_logs` table each run - historical data lost
- `report_snapshots` table doesn't populate `week_start_date`/`week_end_date` columns
- `report_snapshots` is keyed on report run date, not actual week period
- No persistent table for processed weekly aggregates

### Fix
- [x] **2a.** Create `weekly_metrics` table keyed on `(week_start_date, week_end_date)` with UPSERT
- [x] **2b.** Fix `store_snapshot()` to populate `week_start_date` and `week_end_date`
- [x] **2c.** Stop destroying `call_logs` table - use `CREATE TABLE IF NOT EXISTS` + upsert
- [x] **2d.** Move hardcoded DB credentials to environment variables in all files

---

## Phase 3: Email Report to Client (Priority: MEDIUM)

### What's Needed
- `send_report_email()` function using MS Graph API send endpoint
- Configure client recipient address
- Attach or inline the HTML report
- Integrate into pipeline after report generation

### Status: NOT STARTED

---

## Phase 4: Monday Evening Automation (Priority: MEDIUM)

### What's Needed
- Create `run_weekly.py` orchestrator: email ingest -> generate report -> email report -> cleanup
- Set up Windows Task Scheduler for Monday evening execution
- Error handling and logging for each step

### Status: NOT STARTED

---

## Phase 5: Data Integrity Improvements (Priority: LOW)

### What's Needed
- Fix historical JSON log to prevent duplicate entries on same date
- Add trade number database lookup to main analysis pipeline (currently only in ingest_data.py)
- Remove unused `call_analytics_utils.py` or integrate needed functions
- Add data freshness checks

### Status: NOT STARTED
