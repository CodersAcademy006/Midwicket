"""
PyPitch Custom Exceptions

Comprehensive exception hierarchy for robust error handling and debugging.
"""

class PyPitchError(Exception):
    """Base exception for all PyPitch errors."""
    pass

# Data and Ingestion Errors
class DataError(PyPitchError):
    """Base class for data-related errors."""
    pass

class DataIngestionError(DataError):
    """Raised when data ingestion fails."""
    pass

class DataValidationError(DataError):
    """Raised when data validation fails."""
    pass

class SchemaViolationError(DataValidationError):
    """Raised when data doesn't match the expected schema."""
    pass

# Query and Database Errors
class QueryError(PyPitchError):
    """Base class for query-related errors."""
    pass

class QueryTimeoutError(QueryError):
    """Raised when a query exceeds the timeout limit."""
    pass

class DatabaseConnectionError(PyPitchError):
    """Raised when database connection fails."""
    pass

# NOTE: The old `ConnectionError = DatabaseConnectionError` alias has been
# removed because it shadowed Python's built-in ConnectionError (a subclass
# of OSError).  Code that imported `from pypitch.exceptions import
# ConnectionError` would silently receive DatabaseConnectionError instead of
# the built-in, making `except ConnectionError` miss real OS-level errors.
# Use DatabaseConnectionError explicitly.

class QueryExecutionError(QueryError):
    """Raised when query execution fails."""
    pass

# Model and ML Errors
class ModelError(PyPitchError):
    """Base class for model-related errors."""
    pass

class ModelTrainingError(ModelError):
    """Raised when model training fails."""
    pass

class ModelNotFoundError(ModelError):
    """Raised when a requested model is not found."""
    pass

class ModelPredictionError(ModelError):
    """Raised when model prediction fails."""
    pass

# Session and Runtime Errors
class SessionError(PyPitchError):
    """Base class for session-related errors."""
    pass

class SessionInitializationError(SessionError):
    """Raised when session initialization fails."""
    pass

class DependencyError(SessionError):
    """Raised when required dependencies are missing."""
    pass

# Plugin and Extension Errors
class PluginError(PyPitchError):
    """Base class for plugin-related errors."""
    pass

class PluginLoadError(PluginError):
    """Raised when plugin loading fails."""
    pass

class PluginNotFoundError(PluginError):
    """Raised when a requested plugin is not found."""
    pass

# Live and Streaming Errors
class LiveError(PyPitchError):
    """Base class for live data errors."""
    pass

class StreamError(LiveError):
    """Raised when streaming operations fail."""
    pass

class WebhookError(LiveError):
    """Raised when webhook operations fail."""
    pass

# Legacy exceptions for backward compatibility
MatchDataMissing = DataError  # Alias for existing usage
# Backward-compat alias — ingestor.py imports ConnectionError directly.
# New code must use DatabaseConnectionError.
ConnectionError = DatabaseConnectionError  # noqa: A001

__all__ = [
    'PyPitchError',
    'DataError', 'DataIngestionError', 'DataValidationError', 'SchemaViolationError',
    'QueryError', 'QueryTimeoutError', 'DatabaseConnectionError', 'QueryExecutionError',
    'ModelError', 'ModelTrainingError', 'ModelNotFoundError', 'ModelPredictionError',
    'SessionError', 'SessionInitializationError', 'DependencyError',
    'PluginError', 'PluginLoadError', 'PluginNotFoundError',
    'LiveError', 'StreamError', 'WebhookError',
    'MatchDataMissing', 'ConnectionError',
]
