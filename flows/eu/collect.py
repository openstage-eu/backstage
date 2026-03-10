"""EU Procedure Collection

Discovers legislative procedures via SPARQL queries against Cellar.
Saves a timestamped SPARQL snapshot for reproducibility.
"""

import csv
import io

from prefect import flow, task, get_run_logger

from backstage.collection.eu.cellar.sparql.queries import get_procedure_references
from backstage.collection.eu.cellar.sparql import query
from backstage.utils import s3

from datetime import datetime, timezone


@task(retries=2, retry_delay_seconds=300)
def run_sparql_query(sample_mode: bool, sample_limit: int) -> list[dict]:
    """Query Cellar for all procedure references."""
    logger = get_run_logger()

    limit = sample_limit if sample_mode else None
    sparql_query = get_procedure_references(limit=limit)

    if sample_mode:
        logger.info("SAMPLE MODE: limiting to %d procedures", sample_limit)
    else:
        logger.info("PRODUCTION MODE: collecting all procedures")

    raw_csv = query(sparql_query, result_format="csv")
    reader = csv.DictReader(io.StringIO(raw_csv))
    procedures = [row for row in reader]

    logger.info("Collected %d procedures from SPARQL", len(procedures))
    return procedures


@task
def validate_sparql_results(procedures: list[dict]) -> list[dict]:
    """Validate that SPARQL returned a reasonable number of procedures."""
    if not procedures:
        raise ValueError("No procedures found. This likely indicates an API issue.")
    if len(procedures) < 1000 and len(procedures) != 0:
        get_run_logger().warning(
            "Unusually low procedure count: %d (expected ~12500)", len(procedures)
        )
    return procedures


@task
def save_sparql_snapshot(procedures: list[dict], case: str) -> None:
    """Save a timestamped SPARQL snapshot for reproducibility."""
    logger = get_run_logger()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{case}/procedures/state/sparql_snapshots/{today}.json"
    s3.write_json(procedures, key)
    logger.info("Saved SPARQL snapshot: %s (%d procedures)", key, len(procedures))


@flow(name="collect-eu", retries=0)
def collect_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
) -> None:
    """Collect EU procedures from Cellar and save SPARQL snapshot."""
    procedures = run_sparql_query(
        sample_mode=sample_mode,
        sample_limit=sample_limit,
    )
    procedures = validate_sparql_results(procedures)

    if dry_run:
        get_run_logger().info("DRY RUN: skipping S3 writes")
        return

    save_sparql_snapshot(procedures, case)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="eu")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    collect_eu_flow(
        case=args.case,
        sample_mode=args.sample,
        sample_limit=args.sample_limit,
        dry_run=args.dry_run,
    )
