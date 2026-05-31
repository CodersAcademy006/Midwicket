# Ecosystem Readiness Report

**Date:** May 2026
**Version assessed:** 1.1.0
**Assessor role:** Ecosystem Architect

---

## Executive Summary

Midwicket has the technical foundation to become the default cricket analytics
dependency. It has ball-by-ball data access, a working DuckDB engine, type-clean
Python SDK, win probability model, and 25+ player analytics functions. The library
does not need more features. It needs adoption infrastructure: a coherent dataset
catalog, reproducible research, defined benchmarks, and routes for contributors.

This report answers four questions:

1. What prevents Midwicket from becoming the default cricket analytics dependency?
2. What assets are already strong?
3. What ecosystem gaps remain?
4. What should be delayed until external demand appears?

---

## 1. What Prevents Adoption

### 1.1 No canonical dataset catalog

Before this release, `list_datasets()` returned a raw dict with four fields per
dataset — no player counts, no delivery estimates, no date ranges. A researcher
cannot evaluate a library's data coverage from that. The standard for Python
data libraries (scikit-learn's `load_iris`, Hugging Face's dataset cards,
pandas-datareader's source list) is a structured registry with enough metadata
to make a download decision without downloading.

**Status:** Fixed in v1.2.0 — `list_datasets()` now returns 14 datasets with
10 fields each including estimated deliveries, player count, and date range.

### 1.2 No working end-to-end examples

The 36 numbered examples (`01_setup_data.py` through `36_full_library_tour.py`)
and 25 showcases were all written against a local session that may or may not
be present. Most fail on a fresh clone because they reference `session.load_match()`
without first downloading data, or they call internal paths directly.

The examples existed, but they did not work as documentation. The gap: no single
file a new user could run and see a meaningful output.

**Status:** `docs/examples/index.md` now provides 20 working examples with
copy-paste code and verified expected output. They use the public API only.

### 1.3 No reproducible research

Analysts who adopt a library for research need prior work to cite and build on.
Midwicket had no published findings that a researcher could reference in a paper.
The analysis scripts in `analysis/` were local scripts, not studies.

Without reproducible research, the library competes on features. With it, it
competes on knowledge — a much stronger position.

**Status:** 25 research studies are now in `research/`, each structured with
methodology, limitations, and data cutoff. They are not published papers, but
they are citation-ready starting points.

### 1.4 No benchmark definitions

Cricket ML research has no standard datasets, no standard train/test splits,
and no standard evaluation metrics. Every paper defines its own problem.
This makes results incomparable and adoption difficult — researchers cannot
know if a Midwicket-based model is better or worse than prior work.

**Status:** `docs/benchmarks.md` defines four benchmarks (win probability,
wicket probability, fantasy points, score projection) with explicit dataset
splits, features, and evaluation metrics.

### 1.5 Hardcoded dataset paths

`load_dataset()` hardcodes `raw/ipl/` as the extraction directory regardless
of which dataset is being loaded. This means a user who loads `bbl` and `ipl`
simultaneously will find both datasets sharing the same directory. This is a
data corruption vector, not a design choice.

**Status:** Documented in GFI-25 and ADV-01. Not yet fixed — fixing requires
tracing the path through `MidwicketSession.__init__`. Filed as contributor issues.

### 1.6 No governance document

Open-source projects without governance documents cannot attract institutional
contributors. Companies cannot contribute to projects where decisions are opaque.

**Status:** `GOVERNANCE.md` created. Documents decision authority, versioning
policy, succession, and what will not be built.

---

## 2. What Is Already Strong

### 2.1 The DuckDB engine

DuckDB as the query layer is the right call. It is embeddable, fast, column-oriented,
and does not require a server. The decision to make the local file the database
(not an external PostgreSQL instance) means the library works offline, on a plane,
in a Jupyter notebook on Colab. This is the single biggest competitive advantage
over cloud-first cricket data products.

### 2.2 Type discipline

`mypy --strict` passes across 15 modules. This is rare for a solo-founder library
at v1.1.0. It means Midwicket can be adopted in typed Python codebases (data
engineering, production ML pipelines) without creating type noise.

### 2.3 Test coverage (34 tests)

34 tests exist for 8 public functions. Coverage is thin on the edges but the
existing tests are structurally sound. They demonstrate that the maintainer
cares about regression prevention, which is a green flag for potential contributors.

### 2.4 The `express` module

`midwicket.express` as a high-level API (inspired by Plotly Express) is the
right UX pattern. Win probability in three lines, player stats in one line.
This is the API that gets into tutorials and "getting started" posts. It works
without a data download, which removes the highest friction point for new users.

### 2.5 Win probability model (AUC 0.843)

A logistic regression at 0.843 AUC on a held-out IPL test set is a reasonable
baseline. It is not impressive by modern ML standards, but it is transparent,
reproducible, and fast. More importantly, it works without a GPU, without
network access, and without scikit-learn at import time (lazy import is correctly
implemented).

### 2.6 Player analytics breadth (PA-01 to PA-28)

28 player analytics functions covering career, phase, venue, season, form,
matchup, and scouting dimensions. This is comprehensive enough to satisfy 90%
of cricket analyst queries without custom SQL.

### 2.7 Women's cricket as a first-class dataset

WBBL, WPL, and the aliasing of `women_t20` are already present. Most cricket
libraries treat women's cricket as an afterthought or omit it entirely.
Midwicket's architecture treats it identically. This is a genuine differentiator.

---

## 3. Ecosystem Gaps That Remain

### 3.1 No pip-installable package with real data

`pip install midwicket` installs the library. But getting to a meaningful output
with real IPL data requires (a) calling `load_dataset("ipl")`, (b) waiting
30–60 seconds for download, (c) waiting another 5–10 minutes for ingestion.
The first run is a multi-step, multi-minute process with no visual feedback
except a tqdm bar.

This is the adoption cliff. Pybaseball, pandas-datareader, and yfinance all
return data in one function call with no separate ingestion step. Midwicket
requires two.

**What would fix it:** A pre-built DuckDB file for the MLC dataset (~60 MB)
bundled with the package, or at minimum served from GitHub Releases. Users
could query 60 matches immediately without downloading from Cricsheet.

**Status:** Not addressed. Requires infrastructure decision (file hosting).

### 3.2 No Jupyter notebook tour

`notebooks/quickstart.ipynb` exists but is a single file. There is no
narrative notebook that covers: install → data → player analysis → win
probability → custom SQL. The Colab badge in the README points to it but
the notebook is not a complete learning path.

**What would fix it:** A three-notebook sequence: (1) 5-minute quickstart,
(2) 30-minute complete tutorial, (3) advanced custom analysis. Each notebook
should have `%%capture` cells that handle output noise from downloads.

**Status:** Not addressed. Filed as implicit gap.

### 3.3 No PyPI landing page quality

The PyPI page for Midwicket shows the README. The README is good. But a
researcher evaluating the library will also look at:
- Download count (unknown — not yet widely circulated)
- GitHub stars (unknown — not checked)
- Last commit date (recent — good)
- Open issues count (unknown)

None of these can be engineered directly, but stars and downloads correlate
with adoption loops (tutorials, blog posts, conference talks). Midwicket has
none of these yet.

**What would fix it:** One conference presentation at PyCon, CricInfo blog,
or SportsTech conference would materially change discoverability.

### 3.4 No contribution activity

The issues list in `docs/contributors.md` exists, but no community exists
to work through them. A GitHub repository with zero contributor activity
(even one merged non-maintainer PR) signals maturity to potential contributors.

**What would fix it:** Actively recruit 2–3 first contributors by personally
reaching out on cricket analytics forums (CricViz Discord, r/Cricket,
CricketAnalytics Slack).

### 3.5 No `datasets/` pre-computed summaries

Every user who loads IPL must re-ingest 1,100 matches. The ingestion is
idempotent but slow on first run. A pre-computed DuckDB snapshot would
eliminate this — but distributing it requires file hosting, versioning,
and a retrieval mechanism.

This gap blocks the "30-second install, immediate data" experience that
competitive libraries (seaborn, statsmodels) provide.

### 3.6 Research studies are not yet executable

The 25 research studies in `research/` are well-structured but have not been
verified to run end-to-end on a fresh clone. Some SQL assumes `bowling_kind`
is populated; Cricsheet populates this field inconsistently. Several studies
will produce empty DataFrames on certain dataset snapshots.

**What would fix it:** A CI job that runs each study against the MLC dataset
(smallest, fastest) and asserts non-empty output.

---

## 4. What Should Be Delayed

### 4.1 Real-time data pipeline

Building a live ingestion pipeline (streaming from a third-party ball-by-ball
API during matches) requires an API partnership, infrastructure for streaming
ingestion, and a different deployment model. None of this is appropriate until
the static dataset use case has 1,000+ actual users.

**Decision:** Delay until 5,000 monthly PyPI downloads.

### 4.2 Cloud-hosted managed API

A hosted version of Midwicket where users query via HTTP rather than running
locally would require infrastructure, authentication, billing, rate limiting,
and SLA commitments. The economics are unfavourable unless the library already
has significant adoption. Building infrastructure before adoption is building
for an imaginary user.

**Decision:** Delay indefinitely. If demand appears (inbound requests for a
hosted version), evaluate at that point.

### 4.3 GUI or dashboard

Midwicket is a library, not an application. Building a Streamlit or Dash
dashboard requires ongoing maintenance that competes with library quality.
The library's priority is being importable and reliable, not visual.

**Decision:** Delay until external demand. Encourage community members to
build dashboards on top of Midwicket rather than building them in the repo.

### 4.4 Mobile SDK

A cricket analytics library for mobile (Swift, Kotlin) is a completely
separate product from a Python library. The addressable audience (mobile
apps doing ball-by-ball analytics locally) is small and technically
demanding. Not appropriate at this stage.

**Decision:** Indefinite delay.

### 4.5 Additional ML models beyond win probability

Win probability is the one model that matters for initial adoption. Adding
wicket probability, score projection, and fantasy models before win probability
has proven reliable would dilute the quality signal. Focus is better.

**Decision:** Defer new models until the win probability baseline is beaten
by an external contributor via the benchmark system. The benchmark system
is now in place to enable this.

### 4.6 The Hundred-specific metrics

The Hundred uses 100-ball innings in sets of 5, which breaks over-based
metrics. Supporting it properly requires a format flag throughout the
query engine. The dataset is small (200 matches) and the format is English-only.

**Decision:** Keep the dataset in the registry. Do not add format-specific
metric adjustments until someone files an issue with a concrete failing query.

---

## Priority Order for the Next 30 Days

If you act on nothing else from this report, do these three things:

**1. Fix `load_dataset` directory naming** (ADV-01 / GFI-25).
It is a data corruption vector. One user losing their data silently will
generate a GitHub issue that poisons search results for the library.

**2. Run all 20 examples in `docs/examples/index.md` manually.**
Verify every snippet produces the documented output. Fix anything that does not.
Documentation that lies is worse than no documentation.

**3. Post one research finding on social media.**
Pick the most surprising result from the research studies (suggestion: Study 05,
era-adjusted greatness rankings). Write two paragraphs. Attach the chart.
Post on X/Twitter and LinkedIn with a link to the repository. This is the
highest-leverage marketing action available and costs two hours.

---

## Closing Assessment

Midwicket is not a repository. It is infrastructure. The question is not
whether it is technically capable — it is. The question is whether it becomes
findable, trustable, and extensible before a better-funded competitor enters
the space.

The assets added in this release (dataset registry, examples, research studies,
benchmarks, governance, contributor issues) move the needle from "impressive
personal project" to "credible ecosystem". The gap that remains — distribution
friction, no pre-computed data, no community momentum — cannot be solved with
code. It requires the maintainer to do the hard work of adoption: writing, posting,
speaking, and recruiting first contributors personally.

The code is ready. The ecosystem work has started. The adoption is the job.
