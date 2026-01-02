# PyPitch Data Source Adapters

PyPitch supports plug-and-play data ingestion via the Adapter pattern. This document describes the built-in adapters and how to create your own.

## Zero-Config Adapters (Shipped by Default)

- **CricsheetLoader**: Loads Cricsheet JSON data out of the box. No configuration required.
- **RetrosheetAdapter**: (Stub) Example for baseball data.
- **CricAPIAdapter**: (Stub) Example for live cricket API.

## Custom Adapter Interface

To add your own data source, implement the following interface:

```python
from pypitch.data.sources.adapter import DataSource

class MyCustomAdapter(DataSource):
    def get_match_ids(self):
        ...
    def get_match_data(self, match_id):
        ...
    def get_competition_name(self):
        ...
```

## Usage Example

```python
from pypitch.sources.cricsheet_loader import CricsheetLoader
loader = CricsheetLoader()
match_ids = loader.get_match_ids()
data = loader.get_match_data(match_ids[0])
```

## Extending for New Sources
- Use the built-in adapters as templates.
- Register your adapter in your workflow as needed.
