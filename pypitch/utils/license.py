"""
Legal & Attribution Utility for PyPitch.
Ensures proper license and citation for data sources.
"""

import logging as _logging
_logger = _logging.getLogger(__name__)

CRICSHEET_NOTICE = (
    "Data provided by Cricsheet.org (ODbL). "
    "Please attribute correctly in public work."
)


def print_license_notice() -> None:
    """Log the Cricsheet data attribution notice at INFO level."""
    _logger.info(CRICSHEET_NOTICE)
