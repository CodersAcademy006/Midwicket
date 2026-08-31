# Model Versioning and Rollback

Midwicket's `ModelRegistry` supports semantic versioning so production
deployments can promote new model artifacts deliberately and roll back
quickly when a regression is discovered.

This document describes the version scheme, the API surface, the
fallback chain, and the operational procedure for rolling back a model.

## Versioning Scheme

Versions follow [Semantic Versioning 2.0](https://semver.org):

```
MAJOR . MINOR . PATCH
```

| Bump | When to use |
|---|---|
| **MAJOR** | Breaking change to the model's input/output contract. Example: adding a required feature, changing the prediction shape, or swapping algorithm families with materially different latency or memory characteristics. |
| **MINOR** | Backwards-compatible improvements. Example: retraining on more data, hyper-parameter tuning that improves the headline metric, calibration adjustments. Existing clients keep working without changes. |
| **PATCH** | Internal fixes that don't change behaviour materially. Example: dependency bumps for the artifact, retraining on the same data with a fresh random seed, fixing a metadata typo. |

When in doubt, prefer a MINOR bump. PATCH is for truly invisible changes.

Pre-release and build suffixes (`1.0.0-rc1`, `1.0.0+build.42`) are
permitted by the parser but ignored for ordering. Use them sparingly
and only for ephemeral builds.

## Storage Layout

Versioned artifacts live under the registry's `base_path`:

```
~/.midwicket/models/                # base_path (default)
├── registry.json                   # legacy timestamp-based registry
├── versions/
│   └── win_predictor/
│       ├── metadata.json           # active version + per-version metadata
│       ├── 1.0.0.joblib
│       ├── 1.1.0.joblib
│       └── 2.0.0.joblib
└── ...
```

The semver storage is separate from the legacy timestamp-versioned
storage so the two systems coexist without interference.

`metadata.json` for each model:

```json
{
    "active_version": "1.1.0",
    "versions": {
        "1.0.0": {
            "artifact": "1.0.0.joblib",
            "metadata": {"auc_roc": 0.843, "trained_on": "ipl_2008_2021"},
            "registered_at": "2026-05-01T10:00:00"
        },
        "1.1.0": {
            "artifact": "1.1.0.joblib",
            "metadata": {"auc_roc": 0.855, "trained_on": "ipl_2008_2022"},
            "registered_at": "2026-06-01T09:00:00"
        }
    }
}
```

## API

```python
from midwicket.models.registry import ModelRegistry

registry = ModelRegistry()

# Register a new version
registry.register_version(
    model_name="win_predictor",
    version="1.2.0",
    artifact_path="/tmp/my_model.joblib",
    metadata={"auc_roc": 0.86, "trained_on": "ipl_2008_2023"},
)

# Load the active version
model = registry.load_version("win_predictor")

# Load a specific version
model = registry.load_version("win_predictor", "1.0.0")

# List versions (sorted by semver desc, with is_active flag)
for row in registry.list_semver_versions("win_predictor"):
    print(row["version"], row["metadata"], row["is_active"])

# Rollback to an older version
registry.rollback("win_predictor", to_version="1.1.0")

# Inspect active version
print(registry.get_active_version("win_predictor"))  # -> "1.1.0"
```

### Semver Helpers

`midwicket.models.version` exports parser and bumper utilities:

```python
from midwicket.models.version import (
    parse_version,        # "1.2.3" -> (1, 2, 3)
    compare_versions,     # "1.2.3" vs "1.10.0" -> -1
    next_patch,           # "1.2.3" -> "1.2.4"
    next_minor,           # "1.2.3" -> "1.3.0"
    next_major,           # "1.2.3" -> "2.0.0"
    is_valid_version,     # True/False
)
```

## Fallback Chain

When `load_version` is called with a version that is not registered,
the registry walks a deterministic fallback chain:

1. **Previous patch in the same minor.** Looking for `1.2.5`?
   Try `1.2.4`, then `1.2.3`, etc. — the highest registered patch
   below the requested patch in the same minor.
2. **Previous minor in the same major.** No patch hit? Try the
   highest registered version in any earlier minor of the same major.
3. **Previous major.** Still nothing? Fall back to the highest
   registered version in any earlier major.
4. **None.** If no candidate exists, raise `ModelNotFoundError`.

Example: with registered versions `[1.0.0, 1.1.0, 1.2.0, 2.0.0]`:

| Requested | Chosen | Reason |
|---|---|---|
| `1.2.0` | `1.2.0` | exact match |
| `1.2.5` | `1.2.0` | previous patch in 1.2 |
| `1.3.0` | `1.2.0` | previous minor in major 1 |
| `2.0.5` | `2.0.0` | previous patch (only one in major 2) |
| `3.0.0` | `2.0.0` | previous major |
| `0.9.0` | `ModelNotFoundError` | no earlier candidate |

When a fallback is used the registry logs a warning. Treat fallbacks
as alerts: the deployment is running an older version than requested.

## Rollback Procedure

1. **Identify the bad version.** Confirm via metrics that the active
   version regressed.

   ```python
   active = registry.get_active_version("win_predictor")
   ```

2. **Pick a target version.** Choose the highest stable version below
   the bad one.

   ```python
   for row in registry.list_semver_versions("win_predictor"):
       print(row)
   ```

3. **Roll back atomically.** The `rollback` call is guarded by the
   registry lock and persists `metadata.json` atomically.

   ```python
   registry.rollback("win_predictor", to_version="1.1.0")
   ```

4. **Restart consumers** so they pick up the new active version on
   their next `load_version()` call. If your service caches the loaded
   model in process memory, you must explicitly reload — the registry
   does not push.

5. **Investigate.** Inspect the metadata on the bad version, write a
   post-mortem, and decide whether the bad version should be left in
   place (for replay) or deleted.

## Recommended Workflow

1. Train and validate offline.
2. Decide the version bump (MAJOR/MINOR/PATCH).
3. Register via `register_version`. The newly registered version
   becomes active only if it sorts higher than the current active
   version. Newer-but-not-promoted versions stay dormant on disk.
4. Promote explicitly via `rollback(model_name, to_version=new_version)`
   if you registered an older version intentionally.

## Related Files

- `midwicket/models/registry.py` — `ModelRegistry` (extended)
- `midwicket/models/version.py` — semver helpers
- `tests/test_model_versioning.py` — regression tests
- `docs/model_versioning.md` — this document
