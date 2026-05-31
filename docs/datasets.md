# Dataset Catalog

Midwicket downloads and ingests data from [Cricsheet](https://cricsheet.org/),
the open-source ball-by-ball cricket archive. Every dataset listed here is
freely available, versioned, and reproducible.

---

## Quick Reference

```python
from midwicket.datasets import list_datasets, load_dataset

# Print the full catalog
for ds in list_datasets():
    print(ds["name"], "|", ds["est_matches"], "matches", "|", ds["date_range"])

# Load a single competition
session = load_dataset("ipl")
```

---

## Franchise T20 Leagues

### `ipl` — Indian Premier League

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Men |
| Matches | ~1,100 |
| Deliveries | ~480,000 |
| Players | ~750 |
| Date range | 2008–2026 |
| Download size | ~4.5 MB |
| Version | 1.0 |

The IPL is the largest and most commercially significant T20 franchise league.
Coverage is complete from the inaugural 2008 season. Player identities are
standardised via Cricsheet's registry.

```python
session = load_dataset("ipl")
```

---

### `bbl` — Big Bash League

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Men |
| Matches | ~650 |
| Deliveries | ~283,000 |
| Players | ~480 |
| Date range | 2011–2025 |
| Download size | ~2.5 MB |
| Version | 1.0 |

Australia's main domestic T20 competition. Useful for venue analysis of
Australian grounds (MCG, SCG, The Gabba) and comparison with Asian pitch
conditions.

```python
session = load_dataset("bbl")
```

---

### `wbbl` — Women's Big Bash League

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Women |
| Matches | ~550 |
| Deliveries | ~239,000 |
| Players | ~340 |
| Date range | 2015–2025 |
| Download size | ~2.2 MB |
| Version | 1.0 |

The longest-running fully professional women's franchise T20 competition.
The depth of coverage makes WBBL one of the best datasets for women's
batting and bowling analysis.

```python
session = load_dataset("wbbl")
```

---

### `wpl` — Women's Premier League

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Women |
| Matches | ~80 |
| Deliveries | ~35,000 |
| Players | ~120 |
| Date range | 2023–2026 |
| Download size | ~0.3 MB |
| Version | 1.0 |

India's flagship women's franchise league, launched in 2023. Still a young
dataset but growing rapidly. Benchmark comparisons between WPL and WBBL
are meaningful by 2025.

```python
session = load_dataset("wpl")
```

---

### `psl` — Pakistan Super League

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Men |
| Matches | ~350 |
| Deliveries | ~152,000 |
| Players | ~420 |
| Date range | 2016–2025 |
| Download size | ~1.5 MB |
| Version | 1.0 |

Pakistan's national franchise competition. Strong representation of
Pakistani domestic players makes PSL the reference dataset for spin
bowling analysis on subcontinent surfaces.

```python
session = load_dataset("psl")
```

---

### `cpl` — Caribbean Premier League

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Men |
| Matches | ~380 |
| Deliveries | ~165,000 |
| Players | ~460 |
| Date range | 2013–2025 |
| Download size | ~1.6 MB |
| Version | 1.0 |

West Indies franchise T20. Strong source for Caribbean ground characteristics
and for tracking West Indian talent development pathways.

```python
session = load_dataset("cpl")
```

---

### `sa20` — SA20

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Men |
| Matches | ~120 |
| Deliveries | ~52,000 |
| Players | ~180 |
| Date range | 2023–2025 |
| Download size | ~0.5 MB |
| Version | 1.0 |

South Africa's franchise league, launched January 2023. Small dataset
but growing. Useful for South African venue fingerprinting (Newlands,
Wanderers).

```python
session = load_dataset("sa20")
```

---

### `mlc` — Major League Cricket

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Men |
| Matches | ~60 |
| Deliveries | ~26,000 |
| Players | ~90 |
| Date range | 2023–2025 |
| Download size | ~0.2 MB |
| Version | 1.0 |

USA's inaugural professional T20 league. Smallest dataset in the registry.
Best used as a test fixture or to study early-market franchise dynamics.

```python
session = load_dataset("mlc")
```

---

### `hundred` — The Hundred

| Field | Value |
|---|---|
| Format | The Hundred |
| Gender | Both |
| Matches | ~200 |
| Deliveries | ~60,000 |
| Players | ~280 |
| Date range | 2021–2025 |
| Download size | ~0.8 MB |
| Version | 1.0 |

England and Wales' 100-ball competition. The non-standard format
(five-ball sets, 100 balls per innings) means over-based metrics require
adjustment. Cricsheet records The Hundred in standard ball-by-ball JSON,
so raw delivery counts are correct; over-fraction calculations need a
100/20 scaling factor.

```python
session = load_dataset("hundred")
```

---

## International Formats

### `t20is` — T20 Internationals

| Field | Value |
|---|---|
| Format | T20I |
| Gender | Both |
| Matches | ~3,200 |
| Deliveries | ~1,390,000 |
| Players | ~2,800 |
| Date range | 2005–2026 |
| Download size | ~14.5 MB |
| Version | 1.0 |

All men's and women's T20 internationals. The widest cross-national
player coverage in a single T20 dataset. Aliased as `"t20s"` for
backward compatibility.

```python
session = load_dataset("t20is")
# Aliases: "t20s", "t20i", "t20_international"
```

---

### `odis` — One Day Internationals

| Field | Value |
|---|---|
| Format | ODI |
| Gender | Both |
| Matches | ~2,400 |
| Deliveries | ~2,880,000 |
| Players | ~3,200 |
| Date range | 2002–2026 |
| Download size | ~18.0 MB |
| Version | 1.0 |

Complete ODI archive from Cricsheet. The richest dataset for phase-based
analysis (powerplay, middle overs, death) and for studying 50-over
batting strategy evolution. Covers both men's and women's internationals.

```python
session = load_dataset("odis")
# Alias: "odi"
```

---

### `tests` — Test Matches

| Field | Value |
|---|---|
| Format | Test |
| Gender | Both |
| Matches | ~700 |
| Deliveries | ~2,100,000 |
| Players | ~2,400 |
| Date range | 2004–2026 |
| Download size | ~25.0 MB |
| Version | 1.0 |

Ball-by-ball Test match data. Unique in providing multi-day match states.
Useful for cumulative fatigue modelling, session-based analysis, and
pitch degradation studies. Cricsheet coverage begins in 2004.

```python
session = load_dataset("tests")
# Alias: "test", "all_test"
```

---

## Aggregate Datasets

### `all_t20` — All T20 Matches

| Field | Value |
|---|---|
| Format | T20 |
| Gender | Both |
| Matches | ~8,500 |
| Deliveries | ~3,700,000 |
| Players | ~8,000 |
| Date range | 2005–2026 |
| Download size | ~35 MB |
| Version | 1.0 |

Every T20 match in Cricsheet: all franchise leagues, domestic cups, and
international T20Is combined into one session. Ideal for large-scale
population studies where league-specific effects need to be modelled
rather than controlled.

```python
session = load_dataset("all_t20")
```

---

### `all` — Complete Archive

| Field | Value |
|---|---|
| Format | All |
| Gender | Both |
| Matches | ~20,000 |
| Deliveries | ~9,148,000 |
| Players | ~12,000 |
| Date range | 2002–2026 |
| Download size | ~85 MB |
| Version | 1.0 |

The full Cricsheet data lake: T20, ODI, Test, The Hundred, all genders,
all competitions. First-load time is 8–20 minutes depending on bandwidth
and hardware. Subsequent loads use the local DuckDB cache and are instant.

```python
session = load_dataset("all")
```

---

## Alias Reference

| Alias | Resolves to |
|---|---|
| `t20s` | `t20is` |
| `t20i` | `t20is` |
| `t20_international` | `t20is` |
| `odi` | `odis` |
| `test` | `tests` |
| `all_test` | `tests` |
| `all_odi` | `odis` |
| `women_t20` | `wbbl` |

---

## Dataset Notes

**Version numbers** reflect the Midwicket registry schema, not the Cricsheet
upload date. Cricsheet updates source files continuously; Midwicket downloads
the current snapshot at install time. Use `force=True` to refresh:

```python
session = load_dataset("ipl", force=True)
```

**Delivery counts** are estimated from match counts × average balls per
match. Actual counts vary by match result (rain, DLS, early wins).

**Player identifiers** use Cricsheet's stable `player_id` strings (e.g.
`"V Kohli"`, `"JJ Bumrah"`). These are consistent across all datasets.

**The Hundred** uses 100 balls per innings split into sets of 5. Overs
in Midwicket are stored as `floor(ball / 6)`, so metrics derived from
over count need a 5/6 correction for The Hundred data.

---

## Programmatic Access

```python
from midwicket.datasets import list_datasets

# Full metadata for all datasets
for ds in list_datasets():
    print(
        f"{ds['name']:10s}  {ds['est_matches']:5d} matches  "
        f"{ds['est_deliveries']:>10,d} deliveries  {ds['date_range']}"
    )
```

Expected output:

```
ipl            1100 matches     480,000 deliveries  2008–2026
bbl             650 matches     283,000 deliveries  2011–2025
wbbl            550 matches     239,000 deliveries  2015–2025
psl             350 matches     152,000 deliveries  2016–2025
cpl             380 matches     165,000 deliveries  2013–2025
sa20            120 matches      52,000 deliveries  2023–2025
mlc              60 matches      26,000 deliveries  2023–2025
wpl              80 matches      35,000 deliveries  2023–2026
hundred         200 matches      60,000 deliveries  2021–2025
t20is          3200 matches   1,390,000 deliveries  2005–2026
odis           2400 matches   2,880,000 deliveries  2002–2026
tests           700 matches   2,100,000 deliveries  2004–2026
all_t20        8500 matches   3,700,000 deliveries  2005–2026
all           20000 matches   9,148,000 deliveries  2002–2026
```
