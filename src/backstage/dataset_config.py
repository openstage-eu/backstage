"""Dataset configuration factory.

Generates per-case metadata config dicts from minimal input.
Used by case-specific flows to configure packaging and publishing.
"""


def make_dataset_config(
    case: str,
    geographic_coverage: str,
    subject: list[str],
    keywords: list[str],
    author_affiliation: str = "Research Institution",
) -> dict:
    """Build a dataset config dict for a given case.

    Args:
        case: Short case identifier (e.g. "eu", "us", "de").
        geographic_coverage: Geographic scope for Dataverse metadata.
        subject: Dataverse subject categories.
        keywords: Dataset keywords for discovery.
        author_affiliation: Affiliation of the openstage project author.
    """
    return {
        "title_template": f"openstage-{case.lower()} {case.upper()} Legislative Procedures Dataset - {{date}}",
        "description": f"Parsed {case.upper()} legislative procedures from the openstage pipeline",
        "subject": subject,
        "keywords": keywords,
        "geographic_coverage": geographic_coverage,
        "author_name": "openstage project",
        "author_affiliation": author_affiliation,
    }
