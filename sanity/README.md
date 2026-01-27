# Sanity Check & Verification Suite

This folder contains scripts to verify the integrity and accuracy of the Call Analysis Report.

## Scripts

### 1. `verify_data_integrity.py`
Checks if the raw data files were loaded correctly without data loss.
- **Run**: `python sanity/verify_data_integrity.py`
- **Checks**: Row counts, null values, duplicates, date ranges.

### 2. `verify_metrics.py`
Re-calculates the top-level dashboard metrics using independent logic to ensure the report numbers are correct.
- **Run**: `python sanity/verify_metrics.py`
- **Output**: Generates `sanity/dashboard_comparison.md` which you can compare side-by-side with the HTML report.

### 3. `test_core_logic.py`
Runs "unit tests" on the critical logic rules to prove they work as expected.
- **Run**: `python sanity/test_core_logic.py`
- **Tests**: Phone number cleaning (e.g., handling `+353`), Week date assignment logic.

### 4. `generate_sample_audit.py`
Creates a small sample file of calls for manual review.
- **Run**: `python sanity/generate_sample_audit.py`
- **Output**: `sanity/audit_sample.csv`
- **Usage**: Open in Excel and spot-check 5-10 rows to see if the `week` and `customer_type` look correct to you.

## How to Use
1. Run all verification scripts:
   ```bash
   python sanity/verify_data_integrity.py
   python sanity/test_core_logic.py
   python sanity/verify_metrics.py
   ```
2. Open `sanity/dashboard_comparison.md` and compare the numbers with your Dashboard.
3. (Optional) Open `sanity/audit_sample.csv` for manual spot-checking.
