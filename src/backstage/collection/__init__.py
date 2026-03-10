"""
openstage data collection tools

Raw data collection functionality organized by jurisdiction/context.
Currently supports:
- EU: European Union legislative data collection

"""

from __future__ import annotations

from .eu import (
    download_notice,
    build_cellar_resource_url,
    get_procedure_references,
    execute_sparql_query,
)

__all__ = [
    "download_notice",
    "build_cellar_resource_url",
    "get_procedure_references",
    "execute_sparql_query",
]
