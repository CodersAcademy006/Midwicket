# Impact Player Rule Support in PyPitch

PyPitch is architected to support new cricket rules, such as the "Impact Player" rule introduced in IPL 2023, without code changes.

## Key Points
- **No hardcoded player/team limits:** The schema and logic support any number of unique player IDs per match.
- **Registry and loaders are schema-evolution aware:** If new fields or player roles appear, only config/registry updates are needed.
- **Simulation and analytics:** All logic is based on player IDs, not fixed roster size.

## How to Support New Rules
- Update the registry or config to reflect new player participation rules.
- Ingest new schema versions as needed (e.g., Cricsheet post-2023).
- No code changes required for Impact Player or similar rules.

## Example
If a 12th player appears in a match, PyPitch will ingest and analyze their data as usual. Only the registry/config may need an update to recognize the new player.
