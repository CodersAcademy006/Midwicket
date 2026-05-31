# Midwicket Schema Validation Report

This report evaluates the durability and scalability of the core **`BALL_EVENT_SCHEMA`** against future expansion goals:
* **100 Million+ Deliveries**
* **1 Million+ Player/Team/Venue Entities**
* **100 Years of Historical Cricket Records**

---

## ⚡ Core Schema Audit Matrix

The following table compiles the complete audit of every schema field based on actual observed values in the 9.14M delivery database:

| Field Name | PyArrow Type | Observed Min | Observed Max | Recommended Type | Overflow Risk | Architectural Impact & Scale Capacity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`match_id`** | `pa.string()` | `1000881` | `99999` | `pa.string()` | **Zero** | Unlimited capacity. Supports infinite match scale. |
| **`date`** | `pa.date32()` | `2001-12-19` | `2026-05-29` | `pa.date32()` | **Zero** | Supports dates up to Year 5,800,000. Easily accommodates 100+ years of history. |
| **`venue_id`** | `pa.int32()` | `1` | `40,654` | `pa.int32()` | **Zero** | Supports up to 2.14B venues, easily scaling past 1M+ entities. |
| **`inning`** | `pa.int8()` | `1` | `8` | `pa.int8()` | **Zero** | Innings (including consecutive super overs) will never exceed `127`. |
| **`over`** | `pa.int8()` | `0` | `127` | **`pa.int16()`** | 🔴 **HIGH** | **Ingestion Blocker.** Signed 8-bit integer caps at `127`. Test match innings exceeding 127 overs fail validation, omitting 989 matches from database. |
| **`ball`** | `pa.int8()` | `1` | `19` | `pa.int8()` | **Zero** | Even with excessive extras (wides/no-balls), a single over will never exceed `127` balls. |
| **`batter_id`** | `pa.int32()` | `2` | `40,657` | `pa.int32()` | **Zero** | Supports 2.14B unique player IDs. Perfectly scales past 1M+ entities. |
| **`bowler_id`** | `pa.int32()` | `2` | `40,657` | `pa.int32()` | **Zero** | Same as `batter_id`. |
| **`non_striker_id`**| `pa.int32()` | `2` | `42,464` | `pa.int32()` | **Zero** | Same as `batter_id`. |
| **`batting_team_id`**| `pa.int32()` | `21,536` | `42,465` | `pa.int32()` | **Zero** | Upgraded from `int16`. Now supports 2.14B teams. |
| **`bowling_team_id`**| `pa.int32()` | `21,536` | `42,465` | `pa.int32()` | **Zero** | Upgraded from `int16`. Now supports 2.14B teams. |
| **`runs_batter`** | `pa.int32()` | `0` | `8` | `pa.int32()` | **Zero** | Upgraded to prevent DuckDB `SUM` aggregation overflows during analytics. |
| **`runs_extras`** | `pa.int32()` | `0` | `6` | `pa.int32()` | **Zero** | Same as `runs_batter`. |
| **`is_wicket`** | `pa.bool_()` | `false` | `true` | `pa.bool_()` | **Zero** | Boolean type. |
| **`wicket_type`** | `pa.string()` | `N/A` | `N/A` | `pa.string()` | **Zero** | Low-overhead text mapping. |
| **`phase`** | `pa.string()` | `N/A` | `N/A` | `pa.string()` | **Zero** | Low-overhead text mapping. |

---

## 🔍 Future Expansion Load Capability Check

### 1. 100M+ Deliveries Scale Target
* **Database Size Projection:** At the current density, 9.14M deliveries consume **425.4 MB** in DuckDB (using compression). Scale of 100M+ deliveries will occupy approximately **4.6 GB** on disk, which is exceptionally lean and can easily be loaded in memory on standard analysts' laptops.
* **Row Count Constraints:** All columns representing counts and index identifiers (`match_id`, `date`, `venue_id`, `batter_id`, etc.) are either `pa.int32()` or strings, ensuring zero scaling failures when indexing 100M+ lines.

### 2. 1M+ Player & Venue Entities Target
* **Entity Mapping Constraints:** All entity-referencing IDs (`batter_id`, `bowler_id`, `non_striker_id`, `venue_id`, `batting_team_id`, `bowling_team_id`) are signed 32-bit integers (`pa.int32()`), supporting up to **2.14 Billion** unique IDs. They are 100% future-proof and can scale to millions of players, teams, and venues without overflow.

### 3. 100 Years of Cricket History Target
* **Date Representation Constraints:** `pa.date32()` represents dates as the number of days since UNIX epoch (1970-01-01). The type supports a range of approximately **5.8 Million years** in both directions, making it infinitely capable of holding the earliest matches from the 1870s and future matches beyond the year 3000.

---

## 🛠️ Summary of Schema Evolution Actions

1. **🔴 URGENT:** Alter `over` column from `pa.int8()` to `pa.int16()`. This will immediately unlock the ingestion of the remaining 989 Test matches.
2. **🟢 LEAN MAINTENANCE:** Keep `runs_batter` and `runs_extras` as `pa.int32()` despite low observed values (max `8` and `6`) to prevent internal DuckDB columnar casting errors and numeric overflow during bulk aggregations (e.g. `SUM(runs_batter)` over 100 million rows reaches ~130M, exceeding the signed 16-bit integer limit `32,767`).
