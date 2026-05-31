# Midwicket Data Quality Audit Report

This report presents a forensic quality audit conducted across all **9,148,005 deliveries** and **20,888 matches** in the live Midwicket database (`all_v1.0`).

---

## ⚡ Executive Summary of Findings

Overall, the Midwicket live database displays **exceptional integrity**. The referential integrity between delivery events and the global entity registry is **100% perfect**, with zero orphan foreign keys or invalid nulls in required columns. 

However, two domain-specific edge cases were uncovered that require analytical caution:
1. **Retirements Counted as Wickets:** `is_wicket = true` is marked for players who left the crease due to non-dismissal retirements (e.g. `RETIRED_HURT` and `RETIRED_NOT_OUT`). This leads to **25 innings** physically having **11 wickets** in the delivery events table.
2. **Super Overs as Innings 5-8:** Tied matches with consecutive super overs are modeled as innings numbers `5`, `6`, `7`, and `8`. While logically correct, this will cause crashes in external tools expecting a strict `[1, 2, 3, 4]` innings range.

---

## 🔍 Detailed Data Quality Audits

### 1. Duplicate Deliveries
* **Query:** Checked for duplicate combinations of `(match_id, inning, over, ball)` in `ball_events`.
* **Issue Count:** 0
* **Severity:** **None** (Clean)
* **Affected Rows:** 0
* **Audit Verdict:** The database has perfect delivery-level uniqueness. No duplicate ball events exist.

### 2. Duplicate Matches
* **Query:** Checked if any unique match ID corresponds to split or duplicated events.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** All match sequences are distinct and continuous.

### 3. Duplicate Entity IDs in Registry
* **Query:** Checked for duplicate IDs in the `entities` and `aliases` tables in `registry.duckdb`.
* **Issue Count:** 0 in `entities`; 10,658 duplicate `entity_id` values in `aliases`.
* **Severity:** **None** (Expected Behavior)
* **Affected Rows:** 0
* **Audit Verdict:** Zero duplicate global entity IDs exist. The `aliases` table contains 10,658 non-unique `entity_id` mappings, which is the correct design since multiple spellings and alias strings (e.g., "v kohli", "virat kohli") map to a single global player entity ID.

### 4. Orphan Foreign Keys
* **Query:** Evaluated whether any `batter_id`, `bowler_id`, `non_striker_id`, `venue_id`, `batting_team_id`, or `bowling_team_id` in `ball_events` does not exist in the `entities` registry.
* **Issue Count:** 0
* **Severity:** **None** (100% Referential Integrity)
* **Affected Rows:** 0
* **Audit Verdict:** Excellent. Every player, venue, and team referenced in all 9.14 million delivery rows exists in the global identity registry.

### 5. Missing Player Mappings
* **Query:** Checked for `batter_id = 0`, `bowler_id = 0`, `non_striker_id = 0`, or `NULL` values.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** No unmapped or null player IDs are present in the delivery records.

### 6. Missing Venue Mappings
* **Query:** Checked for `venue_id = 0` or `NULL` values.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** Every delivery is perfectly resolved to a registered venue ID.

### 7. Missing Team Mappings
* **Query:** Checked for `batting_team_id = 0`, `bowling_team_id = 0`, or `NULL` values.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** All teams are 100% mapped.

### 8. Invalid Innings Transitions
* **Query:** Checked for deliveries with `inning < 1` or `inning > 4`.
* **Issue Count:** 30 deliveries
* **Severity:** **Low** (Domain-specific representation)
* **Affected Rows:** 30 (spread over tied matches)
* **Audit Findings:** The database contains innings values of `5` (15 rows), `6` (10 rows), `7` (4 rows), and `8` (1 row). 
* **Explanation:** These are **Super Overs** used to break ties. Multiple consecutive super overs in matches like IPL or international ties are correctly modeled as innings `5` through `8`.
* **Fix Recommendation:** Document this design choice in the API reference. Add an optional utility in `load_dataset` to filter out super overs (`inning <= 4`) for analysts wanting clean 1st/2nd innings data.

### 9. Impossible Scorecards
* **Query:** Examined over numbers by competition to identify T20 overs > 20 or ODI overs > 50.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** Over numbers correspond perfectly to the respective formats. Long format (Test) matches correctly contain overs beyond 50 (up to 127 in successfully loaded matches).

### 10. Impossible Wicket Counts (Innings with >10 Wickets)
* **Query:** Grouped `ball_events` by `match_id` and `inning` to check if `SUM(is_wicket)` exceeded 10.
* **Issue Count:** 25 instances
* **Severity:** **Medium** (Semantic Inconsistency)
* **Affected Rows:** 25 innings (e.g. matches `1173070`, `1297723`, `1187017`, `1298028`, `1513352`).
* **Audit Findings:** In these 25 innings, the count of `is_wicket = true` is exactly **11**. 
* **Root Cause:** In Cricsheet's data representation, non-dismissal retirements such as `RETIRED_HURT` and `RETIRED_NOT_OUT` are marked as wicket events in the raw structure and ingested as `is_wicket = true`. Since they do not represent actual bowling dismissals, they inflate the wicket count beyond 10.
* **Fix Recommendation:** Modify the canonicalization script (`midwicket/core/canonicalize.py`) to set `is_wicket = false` for wicket events where `wicket_type` is `RETIRED_HURT` or `RETIRED_NOT_OUT`, as they are not dismissals.

### 11. Impossible Over Counts
* **Query:** Evaluated if over fractions or ball sequences have logical gaps.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** All ball events form sequential, continuous over and ball counts.

### 12. Impossible Ball Numbers
* **Query:** Checked for `ball < 1` or `ball > 20`.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** No invalid ball numbers exist. The maximum observed ball number in an over is 19 (due to multiple consecutive wides/no-balls).

### 13. Negative Values
* **Query:** Checked if `runs_batter < 0`, `runs_extras < 0`, `over < 0`, or `ball < 0`.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** No negative integers exist.

### 14. Null Violations
* **Query:** Checked if required core columns contain nulls.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** Clean schema enforcement.

### 15. Temporal Anomalies
* **Query:** Evaluated if match dates are in the future or predate standard history.
* **Issue Count:** 0
* **Severity:** **None**
* **Affected Rows:** 0
* **Audit Verdict:** All dates are valid, ranging from `2001-12-19` to `2026-05-29`.

---

## 🛠️ Summary of Recommended Fixes

| Issue Identified | Affected Rows | Severity | Recommended Fix |
| :--- | :--- | :--- | :--- |
| **Retirements counted as wickets** | 25 innings | **Medium** | Exclude `RETIRED_HURT`/`RETIRED_NOT_OUT` from triggering `is_wicket = true` in `midwicket/core/canonicalize.py`. |
| **Super Overs as Innings 5-8** | 30 rows | **Low** | Add a documentation note and an easy `filter_super_overs` option to the `load_dataset` API. |
