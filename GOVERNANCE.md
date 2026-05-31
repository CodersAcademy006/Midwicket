# Governance

Midwicket is a solo-founder open-source project. This document describes
how decisions are made and how the project will evolve.

---

## Current State

Midwicket has one maintainer: **Srijan Upadhyay** (srjnupadhyay@gmail.com).

There are no other committers with merge rights. There is no steering committee.
This is intentional — the project is in early adoption phase and a single
decision-maker is faster than a committee.

---

## Decision Authority

| Decision | Who decides |
|---|---|
| Merge a bug fix | Maintainer |
| Merge a documentation PR | Maintainer |
| Add a new public API function | Maintainer, after issue discussion |
| Change the schema of `ball_events` | Maintainer only — breaking change |
| Add a new dependency | Maintainer, with justification |
| Release a new version | Maintainer |
| Change the license | Will not happen |

---

## How Decisions Are Made

1. **Trivial fixes** (typos, broken links, test additions): merge without issue discussion.
2. **Non-trivial changes**: requires an open GitHub issue with at least 3 days for community comment before merge.
3. **Breaking changes** (schema changes, renamed public functions): requires 14 days notice via a GitHub issue labelled `breaking`.

---

## Versioning

Midwicket follows [Semantic Versioning 2.0](https://semver.org/).

- `MAJOR.MINOR.PATCH`
- `PATCH` bumps: bug fixes that do not change public API.
- `MINOR` bumps: new public functions, new datasets, new documentation sections.
- `MAJOR` bumps: breaking changes to the public API or `ball_events` schema.

Current version: `1.1.0`.

---

## Roadmap

The maintainer publishes no public roadmap. The project is feature-frozen
at the library level. What will be actively maintained:

- Bug fixes documented in `PRODUCTION_READINESS_GAPS.md`
- Dataset registry updates (new competitions, metadata corrections)
- Documentation improvements
- Research studies

What will not be built unless external demand materialises:

- Real-time data pipeline
- Cloud hosting or managed API
- GUI or dashboard
- Mobile SDK

---

## Becoming a Committer

There is no committer program currently. If sustained high-quality
contributions merit it, the maintainer may grant merge rights. This has
no defined timeline or criteria.

---

## Conflict Resolution

All disputes are resolved by the maintainer. There is no appeals process.
If you disagree with a decision, you may fork the repository under the MIT License.

---

## Succession

If the maintainer becomes unavailable for more than 90 days without notice,
any contributor may request the GitHub organisation admin to transfer
maintenance rights via the project's GitHub Discussions.

---

## Funding

Midwicket accepts no funding and has no commercial relationships.
There are no sponsors, no ads, and no paid tiers. If this changes,
it will be announced in `CHANGELOG.md`.
