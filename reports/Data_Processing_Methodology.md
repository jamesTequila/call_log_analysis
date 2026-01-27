# Southside Call Analysis Report - Data Processing Methodology

## Executive Summary

This document describes the complete data processing pipeline for the Southside Call Analysis Report, including data sources, cleaning procedures, metric calculations, and analytical methods. This transparency allows stakeholders to verify the analysis's and identify any adjustments needed to meet business requirements.

---

## 1. Data Sources

### 1.1 Main Call Logs
- **Source Files**: `CallLogLastWeek_*.csv` (multiple files covering different time periods)
- **Content**: Detailed call records including call activity, duration, caller information, and status
- **Granularity**: Leg-level data (one row per call segment/transfer)
- **Key Columns**:
  - `Call ID`: Unique identifier for each call
  - `Call Time`: Timestamp of call initiation
  - `From` / `To`: Phone numbers
  - `Direction`: Call direction (Inbound, Inbound Queue, Outbound)
  - `Status`: Call outcome (Answered, Unanswered, etc.)
  - `Ringing` / `Talking`: Duration in HH:MM:SS format
  - `Call Activity Details`: Detailed journey information (queue routing, agent names, termination)

### 1.2 Abandoned Call Logs
- **Source Files**: `AbandonedCalls*.csv` (separate files for abandoned calls)
- **Content**: Calls that were disconnected before being answered
- **Key Columns**:
  - `Call Time`: When the call was received
  - `Caller ID`: Phone number of caller
  - `Queue`: Which queue the call entered
  - `Waiting Time`: How long the caller waited before abandoning
  - `Agent State`: Whether agents were logged in/out
  - `Polling Attempts`: Number of times the system tried to connect to an agent

---

## 2. Data Cleaning Process

### 2.1 Main Call Logs Cleaning

#### Step 1: Remove Invalid Records
- **Action**: Filter out rows where `Call Time` cannot be parsed as a valid date
- **Purpose**: Eliminates summary rows (e.g., "Totals") and corrupted data
- **Implementation**: Convert `Call Time` to datetime, drop rows with NaT values

#### Step 2: Duration Conversion
- **Action**: Convert `Ringing` and `Talking` from HH:MM:SS format to seconds
- **Purpose**: Enable numerical calculations for metrics
- **Formula**: `seconds = hours × 3600 + minutes × 60 + seconds`

#### Step 3: Customer Type Classification
- **Logic**: Extract customer type from `Call Activity Details` field
- **Method**: 
  - Search for pattern: `Inbound: [identifier]`
  - If identifier starts with a **digit** → **Retail** customer
  - If identifier starts with a **letter** → **Trade** customer
  - If no pattern found → **Unknown** (later resolved)
- **Rationale**: Trade customers typically have named accounts; retail customers call from phone numbers

#### Step 4: Direction Filtering
- **Action**: Keep only `Inbound` and `Inbound Queue` calls
- **Purpose**: Focus analysis on incoming customer calls
- **Excluded**: Outbound calls made by the call center

### 2.2 Call-Level Aggregation

Since raw logs contain leg-level data (one row per transfer/queue), we aggregate to call-level:

#### Aggregation Rules (by Call ID):
- **Call Start**: Earliest `Call Time` across all legs
- **From Number**: First `From` value encountered
- **To Number**: First `To` value encountered
- **Directions**: Concatenate all unique directions (sorted)
- **Statuses**: Concatenate all unique statuses (sorted)
- **Ringing Total**: Sum of all `Ringing_sec` values
- **Talking Total**: Sum of all `Talking_sec` values
- **Customer Type**: Resolved using priority logic:
  - If any leg is "trade" → **Trade**
  - Else if any leg is "retail" → **Retail**
  - Else → **Unknown**
- **Call Activity Details**: Concatenate all unique activity details (pipe-separated)

#### Derived Flags:
- **Is Answered**: `talking_total_sec > 0`
- **Is Abandoned**: `talking_total_sec == 0 AND ringing_total_sec > 0`

### 2.3 Abandoned Call Logs Cleaning

#### Step 1: Deduplication
- **Issue Identified**: Source files contained ~6,000 duplicate records
- **Method**: Remove duplicates based on `Caller ID` + `Call Time` combination
- **Result**: ~6,400 rows reduced to ~460 unique abandoned calls

#### Step 2: Phone Number Cleaning
- **Issue**: Caller IDs had `.0` suffix (e.g., "1234567890.0")
- **Action**: Strip `.0` suffix from all `Caller ID` values
- **Purpose**: Enable accurate matching with main log phone numbers

#### Step 3: Customer Type Assignment
- **Method**: Match `Caller ID` against main log phone numbers
- **Logic**:
  - If the caller's number appears in the main log as **Trade** → **Trade**
  - Otherwise → **Retail** (default)
- **Rationale**: Abandoned calls don't have `Call Activity Details`, so we infer type from historical data

---

## 3. Detailed Processing Examples

To illustrate how data cleaning and aggregation work in practice, here are three detailed examples that show how raw data are transformed into final metrics.

### Example 1: Main Call Log - Multi-Leg Call Aggregation

#### Raw Data (Leg-Level Records)
A single customer call that was transferred between agents appears as multiple rows:

| Call ID | Call Time | From | To | Direction | Status | Ringing | Talking | Call Activity Details |
|---------|-----------|------|----|-----------| -------|---------|---------|----------------------|
| 12345 | 2025-11-20 09:15:30 | 07123456789 | 01234567890 | Inbound | Answered | 00:00:15 | 00:02:30 | Inbound: 07123456789 → Sales Queue (501) |
| 12345 | 2025-11-20 09:15:45 | 07123456789 | 01234567891 | Inbound Queue | Answered | 00:00:00 | 00:03:45 | Answered by John Smith, Sales |
| 12345 | 2025-11-20 09:19:30 | 07123456789 | 01234567892 | Inbound Queue | Answered | 00:00:05 | 00:01:20 | Transferred to Jane Doe, Technical Support |
| 12345 | 2025-11-20 09:20:55 | 07123456789 | 01234567892 | Inbound Queue | Answered | 00:00:00 | 00:00:00 | Ended by Jane Doe, Technical Support |

#### Processing Steps:

**Step 1: Duration Conversion**
- Row 1: Ringing = 15 seconds, Talking = 150 seconds
- Row 2: Ringing = 0 seconds, Talking = 225 seconds
- Row 3: Ringing = 5 seconds, Talking = 80 seconds
- Row 4: Ringing = 0 seconds, Talking = 0 seconds

**Step 2: Customer Type Classification**
- Row 1: "Inbound: 07123456789" → Starts with digit "0" → **Retail**
- Rows 2-4: No "Inbound:" pattern → None
- **Result**: At least one leg is Retail → **Final: Retail**

**Step 3: Call-Level Aggregation (Group by Call ID = 12345)**

| Field | Aggregation Logic | Result |
|-------|------------------|--------|
| Call Start | MIN(Call Time) | 2025-11-20 09:15:30 |
| From Number | FIRST(From) | 07123456789 |
| Customer Type | Resolve (Retail > Trade > Unknown) | **Retail** |
| Ringing Total | SUM(Ringing_sec) | 15 + 0 + 5 + 0 = **20 seconds** |
| Talking Total | SUM(Talking_sec) | 150 + 225 + 80 + 0 = **455 seconds** |
| Is Answered | Talking > 0 | **True** |
| Is Abandoned | Talking = 0 AND Ringing > 0 | **False** |
| Call Activity Details | CONCAT unique | "Inbound: 07123456789 → Sales Queue (501) \| Answered by John Smith, Sales \| Transferred to Jane Doe, Technical Support \| Ended by Jane Doe, Technical Support" |

**Step 4: Week Assignment**
- Call Time: 2025-11-20 09:15:30
- Midpoint: 2025-11-20 12:00:00 (example)
- 09:15:30 < 12:00:00 → **Week 2**

**Final Record in Cleaned Dataset:**
```
Call ID: 12345
Week: 2
Customer Type: Retail
Talking Total: 455 seconds (7 minutes 35 seconds)
Status: Answered
Journey: Queue → Sales Agent → Technical Support
```

---

### Example 2: Main Call Log - Trade Customer Identification

#### Raw Data (Single-Leg Call)

| Call ID | Call Time | From | Direction | Status | Ringing | Talking | Call Activity Details |
|---------|-----------|------|-----------|--------|---------|---------|----------------------|
| 67890 | 2025-11-21 14:22:10 | 02087654321 | Inbound | Answered | 00:00:08 | 00:05:42 | Inbound: ABC Supplies Ltd → Sales Queue (501) → Answered by Sarah Johnson, Sales → Ended by 02087654321 |

#### Processing Steps:

**Step 1: Duration Conversion**
- Ringing: 8 seconds
- Talking: 342 seconds

**Step 2: Customer Type Classification**
- Extract: "Inbound: ABC Supplies Ltd"
- First character: "A" (letter)
- **Result: Trade**

**Step 3: Call-Level Aggregation**
- Only one leg, so values pass through directly
- Customer Type: **Trade**
- Talking Total: **342 seconds**

**Step 4: Termination Analysis (from Call Activity Details)**
- "Ended by 02087654321"
- Contains digits, length > 5
- **Terminator: Customer** (caller hung up)

**Step 5: Week Assignment**
- Call Time: 2025-11-21 14:22:10
- Midpoint: 2025-11-20 12:00:00
- 14:22:10 > 12:00:00 → **Week 1**

**Final Record:**
```
Call ID: 67890
Week: 1
Customer Type: Trade
Talking Total: 342 seconds (5 minutes 42 seconds)
Status: Answered
Ended By: Customer
```

**Impact on Metrics:**
- Week 1 Trade Calls: +1
- Week 1 Trade Answered: +1
- Calls Ended by Customer: +1

---

### Example 3: Abandoned Call Log - Deduplication and Customer Type Matching

#### Raw Data (Before Cleaning)

The abandoned call log contained duplicate entries:

| Call Time | Caller ID | Queue | Waiting Time | Agent State | Polling Attempts | Position |
|-----------|-----------|-------|--------------|-------------|------------------|----------|
| 2025-11-19 16:45:22 | 07987654321.0 | Sales Queue (501) | 00:02:15 | Logged Out | 0 | 1 |
| 2025-11-19 16:45:22 | 07987654321.0 | Sales Queue (501) | 00:02:15 | Logged Out | 0 | 1 |
| 2025-11-19 16:45:22 | 07987654321.0 | Sales Queue (501) | 00:02:15 | Logged Out | 0 | 1 |

#### Processing Steps:

**Step 1: Phone Number Cleaning**
- Original: "07987654321.0"
- Strip ".0" suffix
- **Result: "07987654321"**

**Step 2: Deduplication**
- Group by: Caller ID + Call Time
- Original: 3 identical rows
- **After dedup: 1 row**

**Step 3: Customer Type Assignment**
- Check if "07987654321" appears in the main log
- Search main log for `from_number = "07987654321"`
- **Result: NOT FOUND** in main log
- **Default: Retail** (no trade match)

**Step 4: Week Assignment**
- Call Time: 2025-11-19 16:45:22
- Midpoint: 2025-11-20 12:00:00
- 16:45:22 < 12:00:00 → **Week 2**

**Step 5: Agent Availability Analysis**
- Agent State: "Logged Out"
- Polling Attempts: 0
- **Interpretation**: No agents were available to take the call

**Final Record:**
```
Call Time: 2025-11-19 16:45:22
Caller ID: 07987654321
Week: 2
Customer Type: Retail
Waiting Time: 2 minutes 15 seconds
Agent State: Logged Out
Polling Attempts: 0
```

**Impact on Metrics:**
- Total Abandoned Calls: +1 (not +3 due to deduplication)
- Week 2 Retail Abandoned: +1
- Abandoned While Agents Logged Out: +1
- Abandoned with Zero Polling: +1
- Week 2 Retail Abandonment Rate: Increases

**Key Insight from This Example:**
Without deduplication, the abandonment rate would have been artificially inflated by ~600%. This example demonstrates why the cleaning process is critical for accurate metrics.

---

### Example 4: Abandoned Call Log - Trade Customer Match

#### Raw Data

| Call Time | Caller ID | Queue | Waiting Time | Agent State | Polling Attempts |
|-----------|-----------|-------|--------------|-------------|------------------|
| 2025-11-22 10:30:15 | 02087654321.0 | Sales Queue (501) | 00:01:37 | Logged In | 3 |

#### Processing Steps:

**Step 1: Phone Number Cleaning**
- Original: "02087654321.0"
- **Result: "02087654321"**

**Step 2: Customer Type Assignment**
- Check main log for "02087654321"
- **FOUND**: This number appears in main log as **Trade** customer (ABC Supplies Ltd from Example 2)
- **Result: Trade**

**Step 3: Week Assignment**
- Call Time: 2025-11-22 10:30:15
- Midpoint: 2025-11-20 12:00:00
- 10:30:15 > 12:00:00 → **Week 1**

**Step 4: Agent Availability Analysis**
- Agent State: "Logged In"
- Polling Attempts: 3
- **Interpretation**: Agents were available, the system tried 3 times to connect, but the call was still abandoned (likely all agents were busy)

**Final Record:**
```
Call Time: 2025-11-22 10:30:15
Caller ID: 02087654321
Week: 1
Customer Type: Trade
Waiting Time: 1 minute 37 seconds
Agent State: Logged In
Polling Attempts: 3
```

**Impact on Metrics:**
- Week 1 Trade Abandoned: +1
- Week 1 Trade Abandonment Rate: Increases
- Abandoned While Agents Logged In: +1
- **Insight**: Even with agents available, this trade customer abandoned after ~1.5 minutes

**Business Implication:**
This example shows that not all abandonments are due to agents being unavailable. Some occur because all agents are busy with other calls, suggesting potential understaffing during peak periods.

---

### Summary of Examples

| Example | Scenario | Key Transformation | Business Insight |
|---------|----------|-------------------|------------------|
| 1 | Multi-leg call | 4 rows → 1 aggregated record | Shows complete customer journey through transfers |
| 2 | Trade customer | Letter-based classification | Identifies business accounts vs. consumers |
| 3 | Duplicate abandoned | 3 rows → 1 deduplicated record | Prevents 600% inflation of abandonment rate |
| 4 | Trade abandonment | Customer type matching | Reveals abandonment patterns for key accounts |

These examples demonstrate how raw, messy data is transformed into clean, actionable metrics that stakeholders can trust for decision-making.

---

## 4. Week Calculation

### 3.1 Challenge
Data spans only 7 days (Nov 17-23), all within one calendar week (Sunday-Saturday).

### 3.2 Solution: Midpoint-Based Split
Instead of using calendar weeks, we split the data range into two equal periods:

**Formula**:
```
min_date = earliest call timestamp
max_date = latest call timestamp
midpoint = min_date + (max_date - min_date) / 2

If call_time >= midpoint → Week 1 (most recent)
If call_time < midpoint → Week 2 (older)
```

**Example**:
- Data range: Nov 17 07:54 to Nov 23 16:09
- Midpoint: ~Nov 20 12:00
- Week 1: Nov 20 12:00 - Nov 23 16:09 (~3.5 days)
- Week 2: Nov 17 07:54 - Nov 20 12:00 (~3.5 days)

**Benefit**: Ensures both weeks have data for meaningful comparison, even with short time spans.

---

## 4. Metric Calculations

### 4.1 Volume Metrics

#### Total Calls
```
Total Calls = Unique Main Log Calls + Unique Abandoned Calls
```
- Main log calls are deduplicated by `Call ID`
- Abandoned calls are deduplicated by `Caller ID` + `Call Time`

#### Weekly Volume
```
Week 1 Calls = Main Log (Week 1) + Abandoned (Week 1)
Week 2 Calls = Main Log (Week 2) + Abandoned (Week 2)
```

#### Customer Type Breakdown
```
Week 1 Retail Total = Main Log Retail (Week 1) + Abandoned Retail (Week 1)
Week 1 Trade Total = Main Log Trade (Week 1) + Abandoned Trade (Week 1)
```
(Same logic for Week 2)

### 4.2 Abandonment Metrics

#### Overall Abandonment Rate
```
Abandonment Rate = (Total Abandoned Calls / Total Calls) × 100
```

#### By Customer Type
```
Retail Abandonment Rate = (Retail Abandoned / (Retail Answered + Retail Abandoned)) × 100
Trade Abandonment Rate = (Trade Abandoned / (Trade Answered + Trade Abandoned)) × 100
```

#### Weekly Abandonment Rates
```
Week 1 Retail Rate = (Week 1 Retail Abandoned / Week 1 Retail Total) × 100
```
(Same logic for Week 1 Trade, Week 2 Retail, Week 2 Trade)

### 4.3 Caller Journey Metrics

Extracted from `Call Activity Details` field in the main logs:

#### Queue Usage
- **Count**: Calls containing "Queue" in activity details
- **Insight**: Shows how many calls were routed through the queue system

#### Voicemail
- **Count**: Calls containing "Voice Agent" in activity details
- **Insight**: Calls forwarded to voicemail system

#### Out of Hours
- **Count**: Calls containing "Out of office" in activity details
- **Insight**: Calls received when business is closed

#### Call Termination
- **Method**: Parse "Ended by [identifier]" from activity details
- **Classification**:
  - If "Ended by Voice Agent" → **System**
  - If identifier contains digits and length > 5 → **Customer** (phone number)
  - Otherwise → **Agent** (agent name)

### 4.4 Abandoned Call Insights

From abandoned call logs:

#### Agent Availability
- **Logged Out**: Count where `Agent State = "Logged Out"`
- **Logged In**: Count where `Agent State = "Logged In"`
- **Insight**: Shows if abandonments occur due to no agents being available

#### Polling Attempts
- **Zero Polling**: Count where `Polling Attempts = 0`
- **Insight**: Indicates calls abandoned before the system could attempt to connect to any agent

---

## 5. Data Export

### 5.1 CSV Files Generated

| File Name | Content | Purpose |
|-----------|---------|---------|
| `call_logs_cleaned.csv` | Aggregated call-level data with derived metrics | Analysis-ready dataset |
| `call_logs_original.csv` | Raw combined leg-level data from all source files | Audit trail |
| `abandoned_logs_cleaned.csv` | Deduplicated abandoned calls with customer type | Analysis-ready dataset |
| `abandoned_logs_original.csv` | Raw abandoned calls from all source files | Audit trail |
| `verification_data.csv` | Combined main + abandoned with source labels | Cross-validation |

### 5.2 Report Tables
- Display first 10 rows only (for readability)
- Full datasets available via CSV download

---

## 6. Key Assumptions & Decisions

### 6.1 Customer Type Classification
- **Assumption**: Trade customers have named accounts (letters); retail customers call from phone numbers (digits)
- **Verification Needed**: Confirm this logic aligns with actual customer segmentation
- **Alternative**: Provide an explicit customer type mapping file

### 6.2 Abandoned Call Definition
- **Source**: Separate abandoned call log files (not derived from main logs)
- **Assumption**: These files represent true abandonments
- **Note**: Main logs contain some "Unanswered" calls, but these are NOT counted as abandoned

### 6.3 Week Definition
- **Method**: Midpoint-based split (not calendar weeks)
- **Rationale**: Ensures balanced comparison periods
- **Verification Needed**: Confirm this meets business reporting requirements

### 6.4 Disjoint Datasets
- **Finding**: Main logs and abandoned logs have ZERO overlap (confirmed via Call ID matching)
- **Implication**: They are complementary datasets, not redundant
- **Action**: Combined for total volume calculations

---

## 7. Validation Checkpoints

### For Stakeholders to Verify:

1. **Customer Type Logic**:
   - Review sample calls in `call_logs_cleaned.csv`
   - Confirm retail/trade classification is accurate
   - Check if any "unknown" types need resolution

2. **Week Boundaries**:
   - Review `verification_data.csv` to see week assignments
   - Confirm Week 1/Week 2 split aligns with business needs
   - Consider if calendar weeks are preferred

3. **Abandonment Definition**:
   - Review `abandoned_logs_cleaned.csv`
   - Confirm these represent true abandonments
   - Check if any should be excluded (e.g., very short wait times)

4. **Journey Metrics**:
   - Review `Call Activity Details` samples
   - Confirm queue/voicemail/termination parsing is accurate
   - Identify any additional journey patterns to track

5. **Data Completeness**:
   - Compare total call counts with expected volumes
   - Check for any missing time periods
   - Verify all source files were processed

---

## 8. Recommended Next Steps

1. **Validate Customer Type**: Provide feedback on retail/trade classification accuracy
2. **Confirm Week Definition**: Approve midpoint-based split or request calendar weeks
3. **Review Abandonment Criteria**: Confirm abandoned call definition meets requirements
4. **Identify Additional Metrics**: Request any missing KPIs or breakdowns
5. **Schedule Regular Reviews**: Establish cadence for report validation

---

## Contact for Questions

For questions about data processing methodology or to request adjustments, please contact the Tequila AI Analytics Team with specific examples from the CSV files.

**Document Version**: 1.0  
**Last Updated**: 2025-11-29  
**Report Period**: Nov 17-23, 2025
