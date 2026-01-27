# Report Verification Summary
**Date:** 2026-01-27 21:52:42

## 1. Data Integrity Checks
- [x] **Arithmetic Consistency**: PASSED (All sub-components sum correctly to totals)
- [x] **Week Definitions**: Verified (No overlap)
  - This Week: 2026-01-19 to 2026-01-25
  - Last Week: 2026-01-12 to 2026-01-18

## 2. Cross-Section Verification
Ensuring numbers displayed in Data Cards match the Executive Summary and Plots.
| Metric | Data Cards | Exec Summary | Plot (Day Breakdown) | Status |
|---|---|---|---|---|
| This Week Total | 2018 | 2018 | 2018 | ✅ MATCH |
| This Week Retail | 1448 | 1448 | 1448 | ✅ MATCH |
| This Week Trade | 331 | 331 | 331 | ✅ MATCH |
| This Week Abandoned | 239 | 239 | 239 | ✅ MATCH |
| Last Week Total | 1922 | 1922 | 1922 | ✅ MATCH |
| Last Week Retail | 1408 | 1408 | 1408 | ✅ MATCH |
| Last Week Trade | 336 | 336 | 336 | ✅ MATCH |
| Last Week Abandoned | 178 | 178 | 178 | ✅ MATCH |

## 3. Abandoned Calls by Day of Week Plot Verification
Verifying that the sum of the days in the 'Abandoned Calls by Day of Week' plot equals the total abandoned calls reported.
- **This Week Plot Total**: 239
- **This Week Report Total**: 239
- **Status**: ✅ MATCH

## 4. Report Metrics Breakdown
### THIS WEEK
Uses Main Log (2026-01-19 to 2026-01-25) + Abandoned Log (2026-01-19 to 2026-01-25)

**Retail:** 1,448
**Trade:** 331
**Abandoned:** 239
**TOTAL:** 2,018
**Calculation Verified:** 1,448 + 331 + 239 = 2,018 ✓

### LAST WEEK
Uses Main Log (2026-01-12 to 2026-01-18) + Abandoned Log (2026-01-12 to 2026-01-18)

**Retail:** 1,408
**Trade:** 336
**Abandoned:** 178
**TOTAL:** 1,922
**Calculation Verified:** 1,408 + 336 + 178 = 1,922 ✓

### OVERALL TOTAL
**3,940 calls** (2,018 + 1,922)

## 5. Historical Consistency Check
⚠️ **Warnings Detected** (Differences from previous week's report)
- Historical Mismatch for Total Calls: Current=1922, Historical=2340 (Diff: -418)
- Historical Mismatch for Retail Calls: Current=1408, Historical=1710 (Diff: -302)
- Historical Mismatch for Trade Calls: Current=336, Historical=423 (Diff: -87)
- Historical Mismatch for Abandoned Calls: Current=178, Historical=207 (Diff: -29)

> Note: These differences often indicate code logic updates or new data availability.

## 6. Final Result
### ✅ VERIFICATION SUCCESSFUL
The report is internally consistent.