"""EU Dataverse Publishing (thin wrapper around shared.publish)"""

from prefect import flow

from flows.shared.publish import publish_dataset


@flow(name="publish-eu", retries=0)
def publish_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
    package_date: str = "",
) -> None:
    """Publish EU procedures dataset to Harvard Dataverse."""
    publish_dataset(
        case=case,
        geographic_coverage="European Union",
        subject=["Law", "Government", "European Union"],
        keywords=["European Union", "Legislative Procedures", "EUR-Lex", "Open Data"],
        dry_run=dry_run,
        package_date=package_date,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="eu")
    parser.add_argument("--package-date", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    publish_eu_flow(
        case=args.case,
        package_date=args.package_date,
        dry_run=args.dry_run,
    )
