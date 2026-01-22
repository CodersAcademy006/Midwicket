"""
Centralized logging configuration for pypitch.
"""
import logging
import sys
from pathlib import Path

def setup_logging(level: int = logging.INFO, log_file: Path | None = None):
    """
    Configure logging for the entire pypitch package.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Optional file path to write logs to
    """
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    # Set specific loggers to WARNING to reduce noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
