# PyPitch Debug Mode (Eager Execution)

PyPitch supports a global debug mode to make development and debugging easier, especially for users new to lazy evaluation.

## Enabling Debug Mode

```python
import pypitch.config
pypitch.config.set_debug(True)
```

- When `debug=True`, all lazy queries are forced to execute eagerly.
- Errors are raised at the line where the query is written, not deferred.
- This mimics the behavior of Pandas and is recommended for development.

## Disabling Debug Mode

```python
pypitch.config.set_debug(False)
```

- Lazy evaluation is restored for maximum performance.
