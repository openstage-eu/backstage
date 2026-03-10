"""Resolve git commit SHAs for VCS-installed packages.

In production (installed from git via uv), PEP 610 direct_url.json
contains the commit hash. For local editable installs, returns None.
"""

import importlib.metadata
import json
from pathlib import Path


def get_package_commit(package_name: str) -> str | None:
    """Get the git commit SHA for a VCS-installed package.

    Returns the commit hash if the package was installed from a git repo
    (via PEP 610 direct_url.json), or None for editable/PyPI installs.
    """
    try:
        dist = importlib.metadata.distribution(package_name)
    except importlib.metadata.PackageNotFoundError:
        return None

    direct_url_path = Path(str(dist._path)) / "direct_url.json"
    if not direct_url_path.exists():
        return None

    info = json.loads(direct_url_path.read_text())
    vcs_info = info.get("vcs_info", {})
    return vcs_info.get("commit_id")


def get_pipeline_versions() -> dict:
    """Get version info for all pipeline dependencies.

    Returns a dict with package names mapped to their commit SHA
    (if VCS-installed) or version string (if PyPI/editable).
    """
    packages = ["openbasement", "openstage"]
    versions = {}
    for name in packages:
        commit = get_package_commit(name)
        if commit:
            versions[name] = commit
        else:
            try:
                versions[name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                pass
    return versions
