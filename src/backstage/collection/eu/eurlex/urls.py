"""
EUR-Lex URL helper functions.
"""

from typing import Optional
from urllib.parse import urlencode


def build_cellar_resource_url(
    uri: str,
    uri_type: str = "cellar"
) -> str:
    """
    Build direct Cellar resource URL for EUR-Lex content.

    Args:
        uri: Document identifier (CELEX number, ELI URI, or Cellar ID)
        uri_type: Type of URI (celex, eli, cellar)

    Returns:
        Direct resource URL for accessing content via HTTP API
    """
    base_url = "http://publications.europa.eu/resource"

    if uri_type.lower() == "cellar":
        return f"{base_url}/cellar/{uri}"
    elif uri_type.lower() == "celex":
        return f"{base_url}/celex/{uri}"
    elif uri_type.lower() == "eli":
        return f"{base_url}/eli/{uri}"
    else:
        raise ValueError(f"Unsupported URI type: {uri_type}. Use 'cellar', 'celex', or 'eli'")


def build_download_notice_url(
    uri: str,
    uri_type: str = "celex",
    notice_type: str = "tree",
    calling_url: Optional[str] = 'null',
    language: str = "EN",
    **extra_params
) -> str:
    """
    Build EUR-Lex download notice URL (legacy method - use build_cellar_resource_url for direct API).
    """

    legal_content_id = f"{uri_type}:{uri}"

    params = {
        'legalContentId': legal_content_id,
        'noticeType': notice_type,
        'callingUrl': calling_url,
        'lng': language
    }

    params.update(extra_params)

    query_string = urlencode(params)
    return f"https://eur-lex.europa.eu/download-notice.html?{query_string}"
