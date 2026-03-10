"""EU Procedure Collection

Discovers legislative procedures via SPARQL queries against Cellar.
For each procedure, creates or updates a per-procedure metadata file
at {case}/procedures/meta/{proc_ref}.json in S3.

Also saves a timestamped SPARQL snapshot for reproducibility.
"""

import csv
import io
from datetime import datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.utilities.annotations import unmapped

from backstage.collection.eu.cellar.sparql.queries import get_procedure_references
from backstage.collection.eu.cellar.sparql import query
from backstage.utils import s3


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
    key = f"{case}/state/sparql_snapshots/{today}.json"
    s3.write_json(procedures, key)
    logger.info("Saved SPARQL snapshot: %s (%d procedures)", key, len(procedures))


@task
def load_existing_meta(case: str) -> dict[str, dict]:
    """Load all existing per-procedure metadata files from S3.

    Returns a dict mapping proc_ref -> metadata dict.
    """
    logger = get_run_logger()
    prefix = f"{case}/procedures/meta/"
    objects = s3.list_objects(prefix)

    meta = {}
    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue
        proc_ref = key.rsplit("/", 1)[-1].replace(".json", "")
        try:
            meta[proc_ref] = s3.read_json(key)
        except Exception as e:
            logger.warning("Failed to read meta for %s: %s", proc_ref, e)

    logger.info("Loaded %d existing metadata files", len(meta))
    return meta


@task
def write_one_meta(proc: dict, existing_meta: dict[str, dict], case: str) -> str:
    """Write a single procedure metadata file to S3.

    Returns 'created' or 'updated'.
    """
    now = datetime.now(timezone.utc).isoformat()
    proc_ref = proc["procedure"]
    key = f"{case}/procedures/meta/{proc_ref}.json"

    if proc_ref in existing_meta:
        meta = existing_meta[proc_ref]
        meta["last_seen"] = now
        s3.write_json(meta, key)
        return "updated"
    else:
        meta = {
            "rdf_hash": None,
            "first_seen": now,
            "last_seen": now,
        }
        s3.write_json(meta, key)
        return "created"


@task
def aggregate_meta_results(
    results: list[str],
    total: int,
    existing_count: int,
) -> dict:
    """Aggregate results from parallel meta writes."""
    logger = get_run_logger()
    created = sum(1 for r in results if r == "created")
    updated = sum(1 for r in results if r == "updated")
    stats = {
        "sparql_total": total,
        "created": created,
        "updated": updated,
        "existing_not_in_sparql": existing_count - updated,
    }
    logger.info("Metadata update: %s", stats)
    return stats


@flow(name="collect-eu", retries=0, task_runner=ThreadPoolTaskRunner(max_workers=16))
def collect_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
) -> None:
    """Collect EU procedures from Cellar and update per-procedure metadata."""
    procedures = run_sparql_query(
        sample_mode=sample_mode,
        sample_limit=sample_limit,
    )
    procedures = validate_sparql_results(procedures)

    if dry_run:
        get_run_logger().info("DRY RUN: skipping S3 writes")
        return

    logger = get_run_logger()
    save_sparql_snapshot(procedures, case)
    existing_meta = load_existing_meta(case)

    # Process in batches to avoid overwhelming Prefect's ephemeral server
    batch_size = 500
    results = []
    for i in range(0, len(procedures), batch_size):
        batch = procedures[i:i + batch_size]
        logger.info(
            "Meta write batch %d-%d / %d",
            i + 1, min(i + batch_size, len(procedures)), len(procedures),
        )
        futures = write_one_meta.map(batch, unmapped(existing_meta), unmapped(case))
        results.extend(f.result() for f in futures)

    aggregate_meta_results(results, len(procedures), len(existing_meta))


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
