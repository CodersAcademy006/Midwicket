# Contributing to Midwicket

Midwicket is a cricket analytics library. Contributions that make it
more useful for analysts, researchers, and developers are welcome.

This document describes how to contribute code, documentation, research
studies, and benchmark results. Read it once before opening a PR.

---

## What belongs in Midwicket

Midwicket is feature-frozen at the library level. It does not need new
analytics engines, new ML models, or new storage systems. What it does
need is:

- Fixes to documented bugs (see `PRODUCTION_READINESS_GAPS.md`)
- Improved documentation and examples
- New research studies in `research/`
- Benchmark results in `docs/benchmarks.md`
- Better test coverage for edge cases

If you want to add a new analytical function to the core library,
open an issue first and explain the use case. Do not open a PR for
new features without prior discussion.

---

## Setup

**Requirements:** Python 3.9+, git.

```bash
# 1. Fork the repository on GitHub
# 2. Clone your fork
git clone https://github.com/<your-username>/Midwicket.git
cd Midwicket

# 3. Create a virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 4. Install in development mode
pip install -e ".[dev]"

# 5. Verify tests pass
pytest tests/ -x -q
```

Tests should pass in under 60 seconds with no network access required.
If tests require a network connection on a clean clone, that is a bug.

---

## Workflow

1. **Open an issue first** for anything beyond a trivial fix.
   Describe the problem, not the solution. Link to relevant code.

2. **Branch from main.**
   ```bash
   git checkout -b fix/your-description
   ```
   Branch names: `fix/`, `docs/`, `research/`, `test/`.
   Do not use `feature/` — the library is feature-frozen.

3. **Make your changes.** Keep each PR focused on one thing.

4. **Run the checks:**
   ```bash
   pytest tests/ -x -q
   mypy midwicket/ --strict
   ruff check midwicket/ tests/
   ```
   All three must pass before opening a PR.

5. **Write a clear commit message.** One line, present tense, lowercase:
   ```
   fix(datasets): resolve t20s alias mapping to t20is
   docs(examples): add raw sql example to index.md
   research: add study 03 batter age curves
   ```

6. **Open the PR.** Fill in the template. Link the issue.

---

## Code Style

- **Type hints everywhere.** `mypy --strict` must pass.
- **Docstrings on all public functions.** NumPy style.
- **No print statements in library code.** Use `logging.getLogger(__name__)`.
- **No bare `except`.** Catch specific exceptions.
- **Imports:** stdlib, then third-party, then internal.
- **Line length:** 100 characters (configured in `pyproject.toml`).

---

## Tests

Every public function needs at least one test. Tests live in `tests/`.
Use `pytest` only — no `unittest.TestCase`.

```python
def test_list_datasets_returns_all_fields() -> None:
    from midwicket.datasets import list_datasets
    datasets = list_datasets()
    assert len(datasets) >= 10
    for ds in datasets:
        assert "name" in ds
        assert "est_matches" in ds
        assert "date_range" in ds
```

Do not write tests that hit the network. Use fixtures in `tests/conftest.py`.

---

## Documentation

Documentation lives in `docs/`. Write plain English. No marketing language.
Link to source code with `file:line` references where possible.

For examples in `docs/examples/index.md`:
- Include expected output as a comment block.
- Use only public API functions (no internal imports).

---

## Research Studies

Research studies live in `research/`. Each study must:

1. Follow the `_template.py` structure: Question, Methodology, Expected Results, Limitations, Data Cutoff.
2. Run end-to-end with `python research/<filename>.py`.
3. Use only `midwicket.datasets.load_dataset()` — no manual data files.
4. Not require more than 10 minutes to run on a standard laptop.

Number studies sequentially: `26_<short_name>.py`, `27_<short_name>.py`, etc.

---

## Issue Labels

| Label | Meaning |
|---|---|
| `good first issue` | Self-contained, clear scope, under 2 hours |
| `intermediate` | Requires codebase familiarity, 2–8 hours |
| `advanced` | Architecture-level, requires full context |
| `research` | Data analysis study in `research/` |
| `docs` | Documentation only |
| `bug` | Something is broken |
| `test` | Missing or broken test |

---

## What Not to Submit

- New analytics engines or ML model architectures
- New storage backends or REST API endpoints
- Dependency upgrades without a failing test that motivates them
- PRs that touch more than three files without prior discussion

---

## Review Process

PRs are reviewed within 5 business days. The reviewer will either:
- Request changes (specific, actionable feedback)
- Approve and merge
- Close with explanation

---

## License

By contributing, you agree that your contributions will be licensed
under the MIT License that covers this project.
