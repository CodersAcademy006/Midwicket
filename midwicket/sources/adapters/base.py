from typing import Protocol, List, Dict, Any

class BaseAdapter(Protocol):
    def get_match_ids(self) -> List[str]:
        ...
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        ...
