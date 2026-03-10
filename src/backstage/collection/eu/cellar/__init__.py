"""
Cellar Integration Package

Cellar is the EU Publications Office semantic repository that stores
all EU publications metadata and content. Access via SPARQL endpoint.

Modules:
- sparql: Direct SPARQL queries to Cellar endpoint
- sparql.queries: Pre-built query templates
"""

from .sparql import query, execute_sparql_query
from .sparql.queries import get_procedure_references

__all__ = [
    "query",
    "execute_sparql_query",
    "get_procedure_references",
]
