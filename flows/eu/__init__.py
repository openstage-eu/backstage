DEFAULT_ORDER = ["collect", "download", "parse", "package", "publish"]

# Lazy-loaded by the orchestrator via _load_steps().
# Not imported eagerly to avoid RuntimeWarning when running
# individual steps via `python -m flows.eu.<step>`.
_STEP_REGISTRY = {
    "collect": (".collect", "collect_eu_flow"),
    "download": (".download", "download_eu_flow"),
    "parse": (".parse", "parse_eu_flow"),
    "package": (".package", "package_eu_flow"),
    "publish": (".publish", "publish_eu_flow"),
}


def get_steps() -> dict:
    """Import and return all step flow functions."""
    from importlib import import_module

    steps = {}
    for name, (module_name, attr) in _STEP_REGISTRY.items():
        mod = import_module(module_name, package=__name__)
        steps[name] = getattr(mod, attr)
    return steps
