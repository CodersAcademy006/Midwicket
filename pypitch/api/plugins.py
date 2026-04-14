"""
PyPitch Plugin System: Extensible Architecture

Allows third-party packages to inject custom metrics, reports, and data sources.
Uses a simple registry pattern for runtime extensibility.
"""

import importlib
from typing import Dict, Any, Callable, List, Optional
import logging

logger = logging.getLogger(__name__)

class PluginSpec:
    """Specification for a plugin."""
    def __init__(self, name: str, version: str = "1.0.0", description: str = "",
                 entry_point: str = "", dependencies: Optional[List[str]] = None):
        self.name = name
        self.version = version
        self.description = description
        self.entry_point = entry_point
        self.dependencies = dependencies or []

class PluginManager:
    """
    Manages plugin discovery, loading, and execution.

    Plugins can register:
    - Custom metrics (functions that take data and return scalars)
    - Custom reports (functions that generate visualizations)
    - Custom data sources (classes that implement DataSource interface)
    - Custom models (ML models for prediction)
    """

    def __init__(self):
        self._registry: Dict[str, Any] = {}
        self._loaded_plugins: Dict[str, PluginSpec] = {}
        self._metric_functions: Dict[str, Callable] = {}
        self._report_functions: Dict[str, Callable] = {}
        self._data_sources: Dict[str, Any] = {}
        self._models: Dict[str, Any] = {}

    @staticmethod
    def _get_allowlist() -> List[str]:
        """Return the list of permitted plugin top-level package prefixes.

        Reads PYPITCH_PLUGIN_ALLOWLIST (comma-separated).  An empty list means
        no plugins are permitted — production deployments should leave this unset
        unless they explicitly trust a set of plugin packages.
        """
        import os
        raw = os.getenv("PYPITCH_PLUGIN_ALLOWLIST", "")
        return [p.strip() for p in raw.split(",") if p.strip()]

    @staticmethod
    def _validate_module_path(module_path: str, allowlist: List[str]) -> None:
        """Raise ValueError if module_path is unsafe or not in the allowlist.

        Checks performed:
        - Rejects absolute paths, path separators, and shell metacharacters.
        - Rejects paths that are not covered by at least one allowlist prefix.
        """
        import re
        # Reject anything that looks like a filesystem path or shell injection
        if re.search(r'[/\\;|&`$(){}\[\]<>!]', module_path):
            raise ValueError(
                f"Plugin module path {module_path!r} contains unsafe characters."
            )
        if ".." in module_path:
            raise ValueError(
                f"Plugin module path {module_path!r} contains '..' traversal."
            )
        if not allowlist:
            raise ValueError(
                "Plugin loading is disabled: PYPITCH_PLUGIN_ALLOWLIST is empty. "
                "Set it to a comma-separated list of allowed package prefixes to "
                "enable plugins (e.g. PYPITCH_PLUGIN_ALLOWLIST=myplugin)."
            )
        if not any(module_path == pfx or module_path.startswith(pfx + ".") for pfx in allowlist):
            raise ValueError(
                f"Plugin module {module_path!r} is not in the allowlist "
                f"({', '.join(allowlist)}). Add the package prefix to "
                f"PYPITCH_PLUGIN_ALLOWLIST to permit it."
            )

    def discover_plugins(self) -> List[PluginSpec]:
        """Discover available plugins via environment variable.

        Only returns plugins whose module paths are covered by the allowlist.
        If PYPITCH_PLUGINS is set but PYPITCH_PLUGIN_ALLOWLIST is empty, logs
        a warning and returns an empty list instead of loading anything.
        """
        import os
        plugin_list = os.getenv("PYPITCH_PLUGINS", "")
        if not plugin_list:
            return []

        allowlist = self._get_allowlist()
        plugins = []

        for plugin_entry in plugin_list.split(","):
            plugin_entry = plugin_entry.strip()
            if not plugin_entry:
                continue
            if ":" in plugin_entry:
                name, module = plugin_entry.split(":", 1)
                name, module = name.strip(), module.strip()
            else:
                name = module = plugin_entry

            try:
                self._validate_module_path(module, allowlist)
            except ValueError as exc:
                logger.warning("Skipping plugin %r: %s", name, exc)
                continue

            plugins.append(PluginSpec(name=name, entry_point=module))

        return plugins

    def load_plugin(self, plugin_spec: PluginSpec) -> bool:
        """Load a specific plugin after allowlist validation."""
        try:
            allowlist = self._get_allowlist()
            self._validate_module_path(plugin_spec.entry_point, allowlist)

            # Check dependencies
            for dep in plugin_spec.dependencies:
                try:
                    importlib.import_module(dep)
                except ImportError:
                    logger.error("Plugin %s missing dependency: %s", plugin_spec.name, dep)
                    return False

            # Load the plugin module
            module = importlib.import_module(plugin_spec.entry_point)

            # Register plugin components
            if hasattr(module, 'register_metrics'):
                metrics = module.register_metrics()
                self._metric_functions.update(metrics)

            if hasattr(module, 'register_reports'):
                reports = module.register_reports()
                self._report_functions.update(reports)

            if hasattr(module, 'register_data_sources'):
                sources = module.register_data_sources()
                self._data_sources.update(sources)

            if hasattr(module, 'register_models'):
                models = module.register_models()
                self._models.update(models)

            self._loaded_plugins[plugin_spec.name] = plugin_spec
            logger.info(f"Loaded plugin: {plugin_spec.name} v{plugin_spec.version}")
            return True

        except Exception as e:
            logger.error(f"Failed to load plugin {plugin_spec.name}: {e}")
            return False

    def get_metric(self, name: str) -> Optional[Callable]:
        """Get a registered metric function."""
        return self._metric_functions.get(name)

    def get_report(self, name: str) -> Optional[Callable]:
        """Get a registered report function."""
        return self._report_functions.get(name)

    def get_data_source(self, name: str) -> Optional[Any]:
        """Get a registered data source."""
        return self._data_sources.get(name)

    def get_model(self, name: str) -> Optional[Any]:
        """Get a registered model."""
        return self._models.get(name)

    def list_metrics(self) -> List[str]:
        """List all registered metric names."""
        return list(self._metric_functions.keys())

    def list_reports(self) -> List[str]:
        """List all registered report names."""
        return list(self._report_functions.keys())

    def list_data_sources(self) -> List[str]:
        """List all registered data source names."""
        return list(self._data_sources.keys())

    def list_models(self) -> List[str]:
        """List all registered model names."""
        return list(self._models.keys())

# Global plugin manager instance
_plugin_manager = PluginManager()

def get_plugin_manager() -> PluginManager:
    """Get the global plugin manager instance."""
    return _plugin_manager

def register_plugin(category: str):
    """
    Decorator to register a plugin component.

    Usage:
        @register_plugin("metrics")
        def custom_strike_rate(balls_data):
            return calculate_strike_rate(balls_data)
    """
    def decorator(func: Callable) -> Callable:
        if category == "metrics":
            _plugin_manager._metric_functions[func.__name__] = func
        elif category == "reports":
            _plugin_manager._report_functions[func.__name__] = func
        else:
            logger.warning(f"Unknown plugin category: {category}")

        return func
    return decorator

def load_all_plugins() -> int:
    """Load all discovered plugins. Returns number of successfully loaded plugins."""
    plugins = _plugin_manager.discover_plugins()
    loaded_count = 0

    for plugin in plugins:
        if _plugin_manager.load_plugin(plugin):
            loaded_count += 1

    logger.info(f"Loaded {loaded_count}/{len(plugins)} plugins")
    return loaded_count

# Auto-load plugins on import only when PYPITCH_PLUGINS is set AND we are
# not in production mode.  In production, operators must call load_all_plugins()
# explicitly after validating their configuration.
import os as _os
_env = _os.getenv("PYPITCH_ENV", "development")
if _os.getenv("PYPITCH_PLUGINS") and _env != "production":
    try:
        load_all_plugins()
    except Exception as _e:
        logger.warning("Plugin auto-loading failed: %s", _e)

__all__ = [
    'PluginManager',
    'PluginSpec',
    'get_plugin_manager',
    'register_plugin',
    'load_all_plugins'
]