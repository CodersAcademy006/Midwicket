import warnings
from typing import Any

def deprecated_function(message: str, version: str):
    """
    Decorator to mark functions as deprecated.
    
    Usage:
        @deprecated_function("Use new_function instead", "2.0.0")
        def old_function():
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__} is deprecated and will be removed in v{version}. {message}",
                DeprecationWarning,
                stacklevel=2
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator

def deprecated_argument(arg_name: str, message: str, version: str):
    """
    Decorator to mark function arguments as deprecated.
    
    Usage:
        @deprecated_argument("old_param", "Use new_param instead", "2.0.0")
        def my_function(new_param, old_param=None):
            if old_param is not None:
                warnings.warn(...)
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            if arg_name in kwargs and kwargs[arg_name] is not None:
                warnings.warn(
                    f"Argument '{arg_name}' is deprecated and will be removed in v{version}. {message}",
                    DeprecationWarning,
                    stacklevel=2
                )
            return func(*args, **kwargs)
        return wrapper
    return decorator