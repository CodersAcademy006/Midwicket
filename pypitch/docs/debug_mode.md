# PyPitch Debug Mode

PyPitch supports a global debug mode that facilitates development and troubleshooting by enabling eager execution of queries. This is particularly helpful for users new to lazy evaluation patterns or when debugging complex analytics workflows.

## Overview

Debug mode changes PyPitch's execution behavior from lazy (deferred) to eager (immediate), making it easier to identify and fix issues during development.

### Default Behavior (Debug Mode Off)

- Queries are lazily evaluated for optimal performance
- Errors may appear later in the execution flow
- Best for production environments

### Debug Mode Enabled

- All lazy queries execute immediately (eager execution)
- Errors are raised at the exact line where the query is written
- Verbose logging provides detailed execution information
- Similar behavior to libraries like Pandas
- Recommended for development and debugging

## Enabling Debug Mode

There are multiple ways to enable debug mode:

### Method 1: Using Express API

```python
import pypitch.express as px

# Enable debug mode
px.set_debug_mode(True)

# Your code here
session = px.quick_load()
stats = px.get_player_stats("V Kohli")
```

### Method 2: Using Configuration Module

```python
import pypitch.config

# Enable debug mode
pypitch.config.set_debug(True)

# Proceed with your analysis
```

### Method 3: Environment Variable

```bash
export PYPITCH_DEBUG="true"
```

```python
import pypitch as pp
# Debug mode automatically enabled from environment
```

## Disabling Debug Mode

When you're ready to switch back to production mode:

```python
import pypitch.config

# Disable debug mode for optimal performance
pypitch.config.set_debug(False)
```

Or using Express API:

```python
import pypitch.express as px

# Disable debug mode
px.set_debug_mode(False)
```

## Use Cases

### Development & Prototyping

Enable debug mode during development to get immediate feedback:

```python
import pypitch.express as px

px.set_debug_mode(True)

# Any errors will be immediately visible
session = px.quick_load()
stats = px.get_player_stats("V Kohli")  # Error shown here if player not found
```

### Production Deployment

Disable debug mode for optimal performance:

```python
import pypitch.express as px

px.set_debug_mode(False)

# Lazy evaluation for better performance
session = px.quick_load()
```

### Debugging Specific Issues

Toggle debug mode for specific sections:

```python
import pypitch.express as px

# Enable for debugging
px.set_debug_mode(True)
problematic_query()

# Disable after debugging
px.set_debug_mode(False)
normal_operations()
```

## Best Practices

1. **Development**: Enable debug mode for detailed error messages
2. **Testing**: Keep debug mode on to catch issues early
3. **Production**: Disable debug mode for optimal performance
4. **CI/CD**: Configure via environment variables for flexibility

## Performance Impact

Debug mode adds overhead due to:
- Immediate query execution (no batching)
- Verbose logging
- Additional validation checks

**Recommendation**: Only enable debug mode when actively debugging or developing. Disable in production for best performance.

---

For more information on troubleshooting, see the [API Documentation](api.md).
