# PyPitch Adapter Registry

This directory contains the base contract and registry for all data adapters in PyPitch.

## BaseAdapter Contract

All adapters must implement the following interface:

```python
class BaseAdapter(Protocol):
    def get_match_ids(self) -> List[str]: ...
    def get_match_data(self, match_id: str) -> Dict[str, Any]: ...
```

Adapters must return a dict with at least these keys:
- `match_id`
- `format`
- `info`
- `events`
- `raw`

## AdapterRegistry

Register adapters for plug-and-play ingestion:

```python
from .registry import AdapterRegistry
AdapterRegistry.register("my_source", MyAdapter)
AdapterRegistry.get("my_source")  # returns the adapter class
```

## Example: Registering a Custom Adapter

```python
class MyAdapter(BaseAdapter):
    ...
AdapterRegistry.register("my_source", MyAdapter)
```

## Built-in Adapters
- `retrosheet`
- `cricapi`

You can list all registered adapters:

```python
AdapterRegistry.list_adapters()
```
