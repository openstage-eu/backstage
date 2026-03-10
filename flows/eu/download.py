"""EU RDF Download Pipeline

Downloads RDF tree notices from Cellar for each procedure and uploads
them to S3 at {case}/procedures/raw/{proc_ref}.rdf.

Reads procedure references from the latest SPARQL snapshot in
{case}/procedures/state/sparql_snapshots/.

Download modes:
- full (default): download everything, overwrite existing
- incremental: only download procedures without an existing .rdf file

After the initial run, runs up to 3 reconciliation rounds to retry
any missing downloads. Raises RuntimeError if the set is still
incomplete after all rounds.
"""

import requests
from prefect import flow, task, get_run_logger, unmapped
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff

from backstage.utils import s3
from backstage.utils.user_agent import get_user_agent


CELLAR_BASE_URL = "https://publications.europa.eu/resource/procedure"
MAX_RECONCILIATION_ROUNDS = 3


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
def load_latest_snapshot(case: str) -> list[dict]:
    """Load the latest SPARQL snapshot from S3.

    Lists {case}/procedures/state/sparql_snapshots/ keys, picks the latest by date
    (key name is {date}.json), reads and returns the list of procedure dicts.
    """
    logger = get_run_logger()
    prefix = f"{case}/procedures/state/sparql_snapshots/"
    objects = s3.list_objects(prefix)

    snapshot_keys = [
        obj["Key"] for obj in objects if obj["Key"].endswith(".json")
    ]
    if not snapshot_keys:
        raise ValueError(f"No SPARQL snapshots found at {prefix}")

    latest_key = sorted(snapshot_keys)[-1]
    logger.info("Loading SPARQL snapshot: %s", latest_key)
    return s3.read_json(latest_key)


@task
def list_procedure_refs(
    snapshot: list[dict], sample_mode: bool, sample_limit: int,
) -> list[str]:
    """Extract procedure references from a SPARQL snapshot."""
    logger = get_run_logger()
    proc_refs = [row["procedure"] for row in snapshot]

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

    if mode == "incremental" and s3.exists(rdf_key):
        return {"proc_ref": proc_ref, "status": "skipped"}

    content = fetch_rdf_tree(proc_ref)
    s3.upload_bytes(content, rdf_key)

    return {"proc_ref": proc_ref, "status": "downloaded"}


def _list_raw_refs(case: str) -> set[str]:
    """List procedure refs that have raw RDF files on S3."""
    prefix = f"{case}/procedures/raw/"
    objects = s3.list_objects(prefix)
    return {
        obj["Key"].rsplit("/", 1)[-1].replace(".rdf", "")
        for obj in objects
        if obj["Key"].endswith(".rdf")
    }


def _run_downloads(
    proc_refs: list[str], case: str, mode: str, logger,
) -> None:
    """Run .map() downloads, swallowing individual failures."""
    logger.info("Downloading %d procedures", len(proc_refs))
    futures = download_one.map(proc_refs, unmapped(case), unmapped(mode))
    for future in futures:
        try:
            future.result()
        except Exception:
            pass


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

    After the initial batch, reconciles against S3 up to 3 times
    to retry any missing downloads. Raises RuntimeError if the set
    is still incomplete.

    Args:
        mode: "full" (re-download everything) or "incremental" (only new procedures).
    """
    if dry_run:
        get_run_logger().info("DRY RUN: skipping downloads")
        return

    logger = get_run_logger()
    snapshot = load_latest_snapshot(case)
    proc_refs = list_procedure_refs(snapshot, sample_mode, sample_limit)
    expected = set(proc_refs)

    # Initial run
    _run_downloads(proc_refs, case, mode, logger)

    # Reconciliation loop
    for round_num in range(1, MAX_RECONCILIATION_ROUNDS + 1):
        on_s3 = _list_raw_refs(case)
        missing = expected - on_s3
        if not missing:
            logger.info("All %d procedures downloaded successfully", len(expected))
            return

        logger.warning(
            "Reconciliation round %d: %d missing out of %d",
            round_num, len(missing), len(expected),
        )
        _run_downloads(sorted(missing), case, mode, logger)

    # Final check
    on_s3 = _list_raw_refs(case)
    missing = expected - on_s3
    if missing:
        raise RuntimeError(
            f"Download incomplete after {MAX_RECONCILIATION_ROUNDS} reconciliation "
            f"rounds: {len(missing)} procedures still missing. "
            f"Examples: {sorted(missing)[:5]}"
        )
    logger.info("All %d procedures downloaded successfully", len(expected))


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
