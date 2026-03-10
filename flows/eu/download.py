"""EU RDF Download Pipeline

Downloads RDF tree notices from Cellar for each procedure and uploads
them to S3 at {case}/procedures/raw/{proc_ref}.rdf.

Reads procedure references from per-procedure metadata files in
{case}/procedures/meta/.

Download modes:
- full (default): download everything, overwrite existing
- incremental: only download procedures without an existing .rdf file
"""

import hashlib

import requests
from prefect import flow, task, get_run_logger, unmapped
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff

from backstage.utils import s3
from backstage.utils.user_agent import get_user_agent


CELLAR_BASE_URL = "https://publications.europa.eu/resource/procedure"


def fetch_rdf_tree(proc_ref: str) -> bytes:
    """Fetch RDF tree for a procedure from Cellar."""
    url = f"{CELLAR_BASE_URL}/{proc_ref}"
    response = requests.get(
        url,
        timeout=60,
        headers={
            "User-Agent": get_user_agent(),
            "Accept": "application/rdf+xml;notice=tree",
        },
    )
    response.raise_for_status()
    return response.content


@task
def list_procedure_refs(case: str, sample_mode: bool, sample_limit: int) -> list[str]:
    """List all procedure references from S3 meta key names (no file reads)."""
    logger = get_run_logger()

    prefix = f"{case}/procedures/meta/"
    objects = s3.list_objects(prefix)

    proc_refs = []
    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue
        proc_ref = key.rsplit("/", 1)[-1].replace(".json", "")
        proc_refs.append(proc_ref)

    if sample_mode:
        proc_refs = proc_refs[:sample_limit]

    logger.info("Found %d procedures to download", len(proc_refs))
    return proc_refs


@task(
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=0.5,
)
def download_one(proc_ref: str, case: str, mode: str) -> dict:
    """Download a single procedure's RDF tree from Cellar and upload to S3."""
    rdf_key = f"{case}/procedures/raw/{proc_ref}.rdf"
    meta_key = f"{case}/procedures/meta/{proc_ref}.json"

    if mode == "incremental" and s3.exists(rdf_key):
        return {"proc_ref": proc_ref, "status": "skipped"}

    content = fetch_rdf_tree(proc_ref)
    content_hash = hashlib.sha256(content).hexdigest()

    s3.upload_bytes(content, rdf_key)

    meta = s3.read_json(meta_key)
    meta["rdf_hash"] = content_hash
    s3.write_json(meta, meta_key)

    return {"proc_ref": proc_ref, "status": "downloaded"}


@task
def aggregate_results(results: list[dict]) -> dict:
    """Aggregate download results into stats."""
    logger = get_run_logger()
    stats = {
        "total": len(results),
        "downloaded": sum(1 for r in results if r["status"] == "downloaded"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
    }
    logger.info("Download complete: %s", stats)
    return stats


@task
def validate_downloads(stats: dict) -> None:
    """Check failure rate is acceptable."""
    logger = get_run_logger()
    total = stats["total"]
    if total > 0 and stats["failed"] / total > 0.1:
        raise ValueError(
            f"High failure rate: {stats['failed']}/{total} "
            f"({stats['failed']/total:.1%})"
        )
    if stats["failed"] > 0:
        logger.warning("Failed downloads: %d/%d", stats["failed"], total)
    logger.info("Download validation passed")


@flow(
    name="download-eu",
    retries=0,
    task_runner=ThreadPoolTaskRunner(max_workers=16),
)
def download_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
    mode: str = "full",
) -> None:
    """Download RDF tree notices for all EU procedures.

    Args:
        mode: "full" (re-download everything) or "incremental" (only new procedures).
    """
    if dry_run:
        get_run_logger().info("DRY RUN: skipping downloads")
        return

    logger = get_run_logger()
    proc_refs = list_procedure_refs(case, sample_mode, sample_limit)

    # Process in batches to avoid overwhelming Prefect's ephemeral server
    batch_size = 200
    results = []
    for i in range(0, len(proc_refs), batch_size):
        batch = proc_refs[i:i + batch_size]
        logger.info(
            "Download batch %d-%d / %d",
            i + 1, min(i + batch_size, len(proc_refs)), len(proc_refs),
        )
        futures = download_one.map(batch, unmapped(case), unmapped(mode))
        for future in futures:
            try:
                results.append(future.result())
            except Exception:
                results.append({"proc_ref": "unknown", "status": "failed"})

    stats = aggregate_results(results)
    validate_downloads(stats)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="eu")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
    )
    args = parser.parse_args()

    download_eu_flow(
        case=args.case,
        sample_mode=args.sample,
        sample_limit=args.sample_limit,
        dry_run=args.dry_run,
        mode=args.mode,
    )
