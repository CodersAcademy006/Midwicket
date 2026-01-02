"""
Legal & Attribution System for PyPitch.

Ensures proper attribution to data sources and handles licensing.
Critical for enterprise adoption and legal compliance.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import warnings

@dataclass
class DataAttribution:
    """Attribution information for a dataset."""
    source: str  # e.g., "Cricsheet", "Sportmonks"
    license_type: str  # e.g., "ODbL", "CC BY-SA"
    license_url: str
    attribution_text: str
    last_updated: datetime
    version: str

class AttributionManager:
    """
    Manages data source attributions and licensing.

    Ensures users properly attribute data sources in their work.
    """

    def __init__(self):
        self.attributions: Dict[str, DataAttribution] = {}

        # Initialize with known data sources
        self._init_known_sources()

    def _init_known_sources(self):
        """Initialize attribution data for known sources."""
        self.attributions['cricsheet'] = DataAttribution(
            source="Cricsheet",
            license_type="Open Database License (ODbL)",
            license_url="https://opendatacommons.org/licenses/odbl/",
            attribution_text="Data provided by Cricsheet.org (ODbL). Please attribute correctly in public work.",
            last_updated=datetime(2024, 1, 1),
            version="v1.0"
        )

        self.attributions['sportmonks'] = DataAttribution(
            source="Sportmonks",
            license_type="Commercial License",
            license_url="https://sportmonks.com/license",
            attribution_text="Data provided by Sportmonks. Commercial license required for redistribution.",
            last_updated=datetime(2024, 1, 1),
            version="v1.0"
        )

    def get_attribution(self, source: str) -> Optional[DataAttribution]:
        """Get attribution information for a data source."""
        return self.attributions.get(source.lower())

    def display_attribution_warning(self, source: str) -> None:
        """
        Display attribution warning when data is used.

        This is called automatically during data ingestion.
        """
        attribution = self.get_attribution(source)
        if attribution:
            warnings.warn(
                attribution.attribution_text,
                UserWarning,
                stacklevel=2
            )

    def generate_citation(self, source: str, format: str = "text") -> str:
        """
        Generate proper citation for a data source.

        Args:
            source: Data source name
            format: Citation format ("text", "bibtex", "apa")

        Returns:
            Formatted citation string
        """
        attribution = self.get_attribution(source)
        if not attribution:
            return f"Data source: {source}"

        if format == "bibtex":
            return f"""
@misc{{{source.lower()}_data,
  title={{{source} Cricket Data}},
  author={{{source} Team}},
  year={{{attribution.last_updated.year}}},
  url={{{attribution.license_url}}},
  note={{{attribution.attribution_text}}}
}}
            """.strip()

        elif format == "apa":
            return f"{source} Team. ({attribution.last_updated.year}). {source} cricket data [Data set]. {attribution.license_url}"

        else:  # text format
            return attribution.attribution_text

    def check_license_compatibility(self, sources: list) -> Dict[str, Any]:
        """
        Check license compatibility when combining multiple data sources.

        Returns compatibility analysis and recommendations.
        """
        licenses = []
        for source in sources:
            attribution = self.get_attribution(source)
            if attribution:
                licenses.append(attribution.license_type)

        # Analyze compatibility
        has_odbl = any("ODbL" in lic for lic in licenses)
        has_commercial = any("Commercial" in lic for lic in licenses)

        result = {
            "compatible": True,
            "warnings": [],
            "recommendations": []
        }

        if has_odbl and has_commercial:
            result["compatible"] = False
            result["warnings"].append("Cannot combine ODbL and Commercial licensed data")
            result["recommendations"].append("Use only one license type per analysis")

        if len(set(licenses)) > 1:
            result["warnings"].append("Multiple license types detected")
            result["recommendations"].append("Document all licenses in your attribution")

        return result

class MatchAttribution:
    """Attribution metadata for individual matches."""

    def __init__(self, match_id: str, source: str, license_info: Dict[str, Any]):
        self.match_id = match_id
        self.source = source
        self.license_info = license_info
        self.created_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "match_id": self.match_id,
            "source": self.source,
            "license": self.license_info,
            "created_at": self.created_at.isoformat(),
            "attribution_manager": AttributionManager.__name__
        }

# Global instance
_attribution_manager = AttributionManager()

def get_attribution(source: str) -> Optional[DataAttribution]:
    """Get attribution for a data source."""
    return _attribution_manager.get_attribution(source)

def display_attribution(source: str) -> None:
    """Display attribution warning for a data source."""
    _attribution_manager.display_attribution_warning(source)

def generate_citation(source: str, format: str = "text") -> str:
    """Generate citation for a data source."""
    return _attribution_manager.generate_citation(source, format)

def check_license_compatibility(sources: list) -> Dict[str, Any]:
    """Check license compatibility for multiple sources."""
    return _attribution_manager.check_license_compatibility(sources)