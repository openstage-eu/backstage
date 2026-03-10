"""
EUR-Lex Notice Download Module
"""

from typing import Optional

import requests

from backstage.utils.retry import with_retry
from backstage.utils.user_agent import get_user_agent
from .urls import build_cellar_resource_url


def download_notice(uri: str,
                   uri_type: str = "cellar",
                   notice_type: str = "tree",
                   language: Optional[str] = None,
                   response_format: str = "xml") -> bytes:
    """
    Download notice directly from Cellar using the simplified HTTP API.

    Args:
        uri: Document identifier (CELEX number, ELI URI, or Cellar ID)
        uri_type: Type of URI (celex, eli, cellar)
        notice_type: Type of notice (tree, summary, etc.) - passed in Accept header
        language: Language code (EN, FR, DE, etc.) - optional
        response_format: Response format ("xml" for application/xml or "rdf+xml" for application/rdf+xml)

    Returns:
        Raw notice content as bytes
    """

    download_url = build_cellar_resource_url(uri, uri_type)

    if response_format.lower() == "rdf+xml":
        content_type = "application/rdf+xml"
    else:
        content_type = "application/xml"

    accept_parts = [content_type]

    if notice_type:
        accept_parts.append(f"notice={notice_type}")

    if language:
        accept_parts.append(f"lang={language}")

    accept_header = ";".join(accept_parts)

    @with_retry(max_attempts=3, base_delay=1.0)
    def _download():
        response = requests.get(
            download_url,
            timeout=30,
            headers={
                'User-Agent': get_user_agent(),
                'Accept': accept_header
            }
        )
        response.raise_for_status()
        return response.content

    return _download()
