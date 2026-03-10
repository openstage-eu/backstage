"""EU Dataset Packaging (thin wrapper around shared.package)"""

from prefect import flow, get_run_logger

from backstage.utils.versions import get_pipeline_versions
from flows.shared.package import build_dataset_package


@flow(name="package-eu", retries=0)
def package_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
    release: str = "",
) -> None:
    """Package parsed EU procedures into a ZIP dataset."""
    if dry_run:
        get_run_logger().info("DRY RUN: skipping packaging")
        return

    pipeline_versions = get_pipeline_versions()

    build_dataset_package(
        case=case,
        release=release,
        dataset_name="openstage-eu",
        dataset_label="EU Procedures",
        pipeline_versions=pipeline_versions,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="eu")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--release", default="", help="Release period (YYYY.MM)")
    args = parser.parse_args()

    package_eu_flow(
        case=args.case,
        sample_mode=args.sample,
        sample_limit=args.sample_limit,
        dry_run=args.dry_run,
        release=args.release,
    )
