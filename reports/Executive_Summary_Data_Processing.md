# Southside Call Analysis Report - Executive Summary

**Report Period**: Nov 17-23, 2025  
**Purpose**: Transparent overview of data processing methodology for executive review

---

## Data Sources

- **Main Call Logs**: Detailed records of all inbound calls (answered and unanswered)
- **Abandoned Call Logs**: Separate records of calls disconnected before being answered
- **Key Finding**: These datasets are completely separate (no overlap), requiring a combination for an accurate total volume

---

## Critical Data Quality Issues Addressed

### 1. Duplicate Records
- **Issue**: Abandoned call logs contained ~6,000 duplicate entries
- **Impact**: Would have inflated abandonment rate by 600%
- **Solution**: Automated deduplication reduced 6,400 rows to 460 unique abandoned calls

### 2. Phone Number Formatting
- **Issue**: Numbers had `.0` suffix (e.g., "1234567890.0")
- **Solution**: Cleaned all phone numbers for accurate customer matching

### 3. Multi-Leg Calls
- **Issue**: Transferred calls appeared as multiple rows (one per agent)
- **Solution**: Aggregated to single call-level records to avoid double-counting

---

## Key Methodology Decisions

### Customer Type Classification
- **Trade**: Identified by named accounts (e.g., "ABC Supplies Ltd")
- **Retail**: Phone number-based callers
- **Abandoned Calls**: Matched against main log; unmatched default to Retail

### Week Definition
- **Challenge**: Data spans only 7 days (one calendar week)
- **Solution**: Split data at midpoint for Week 1 vs Week 2 comparison
  - Week 1: Most recent 3.5 days
  - Week 2: Older 3.5 days
- **Alternative**: Can switch to calendar weeks if preferred

### Abandonment Rate Calculation
```
Abandonment Rate = Abandoned Calls / (Answered + Abandoned)
```
- Calculated overall and by customer type (Retail/Trade)
- Calculated for each week

---

## Metrics Derived

### Volume Metrics
- Total calls (answered + abandoned)
- Weekly breakdown (Week 1 vs Week 2)
- Customer type breakdown (Retail vs Trade)

### Performance Metrics
- Overall abandonment rate
- Abandonment by customer type
- Abandonment by week

### Operational Insights
- **Caller Journey**: Queue usage, voicemail routing, call termination patterns
- **Agent Availability**: 60% of abandonments occurred when agents were logged out
- **Out of Hours**: Calls received when business is closed

---

## Data Validation Checkpoints

### Recommended CEO Review Points:

1. **Customer Classification**: Verify Trade vs Retail logic aligns with business definitions
2. **Week Boundaries**: Confirm midpoint-based split meets reporting needs (vs calendar weeks)
3. **Abandonment Definition**: Validate that abandoned call criteria match business expectations
4. **Key Metrics**: Review total call volumes against expected ranges

---

## Downloadable Data Files

All raw and cleaned datasets are available for audit:
- `call_logs_cleaned.csv` - Processed call records
- `call_logs_original.csv` - Raw source data
- `abandoned_logs_cleaned.csv` - Deduplicated abandoned calls
- `abandoned_logs_original.csv` - Raw abandoned data

---

## Business Impact

### Before Data Cleaning:
- Abandonment rate: ~54% (inflated by duplicates)
- Customer types: Misclassified due to formatting issues
- Call volumes: Double-counted due to multi-leg records

### After Data Cleaning:
- Abandonment rate: ~10% (accurate)
- Customer types: Correctly classified Trade vs Retail
- Call volumes: Accurate unique call counts

---

## Recommendations

1. **Approve Methodology**: Confirm data processing approach meets business requirements
2. **Validate Classifications**: Spot-check customer type assignments in downloadable CSVs
3. **Establish Baseline**: Use this period as a benchmark for future comparisons
4. **Schedule Reviews**: Monthly validation of methodology as data patterns evolve

---

## Questions or Concerns?

For detailed methodology, see complete documentation: `Data_Processing_Methodology.md`

**Contact**: Tequila AI Analytics Team for methodology questions or adjustment requests
