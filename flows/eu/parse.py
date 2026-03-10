"""EU Procedure Parsing Pipeline

Reads downloaded RDF files from S3 at {case}/procedures/raw/{proc_ref}.rdf,
parses them with openbasement, converts to openstage EUProcedure models via
the adapter, and writes model_dump() output to {case}/procedures/parsed/{proc_ref}.json.

Always parses every RDF file. Monthly full rebuild makes skip logic
unnecessary. Records dependency commit SHAs for reproducibility.

After the initial run, runs up to 3 reconciliation rounds to retry
any missing parses. Raises RuntimeError if the set is still incomplete
after all rounds.
"""

from io import BytesIO

from prefect import flow, task, get_run_logger
from prefect.task_runners import ProcessPoolTaskRunner
from prefect.utilities.annotations import unmapped

from backstage.utils import s3
from backstage.utils.versions import get_pipeline_versions


MAX_RECONCILIATION_ROUNDS = 3


@task
def list_parseable_refs(
    case: str,
    sample_mode: bool,
    sample_limit: int,
) -> list[str]:
    """List procedure refs that have downloaded RDF files (no file reads)."""
    logger = get_run_logger()

    prefix = f"{case}/procedures/raw/"
    objects = s3.list_objects(prefix)

    proc_refs = []
    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".rdf"):
            continue
        proc_ref = key.rsplit("/", 1)[-1].replace(".rdf", "")
        proc_refs.append(proc_ref)

    if sample_mode:
        proc_refs = proc_refs[:sample_limit]

    logger.info("Found %d procedures with RDF data to parse", len(proc_refs))
    return proc_refs


@task(retries=1, retry_delay_seconds=5)
def parse_one(proc_ref: str, case: str) -> dict:
    """Parse a single procedure's RDF and write parsed JSON to S3.

    Returns a dict with proc_ref and status (parsed | failed).
    """
    from rdflib import Graph
    from openbasement import extract
    from openstage.models.eu import EUProcedure

    rdf_key = f"{case}/procedures/raw/{proc_ref}.rdf"
    parsed_key = f"{case}/procedures/parsed/{proc_ref}.json"

    rdf_bytes = s3.read_bytes(rdf_key)
    graph = Graph()
    graph.parse(BytesIO(rdf_bytes), format="xml")
    result = extract(graph, template="eu_procedure")

    procedure = EUProcedure.from_openbasement(result[0])
    s3.write_json(procedure.model_dump(), parsed_key)

    return {"proc_ref": proc_ref, "status": "parsed"}


def _list_parsed_refs(case: str) -> set[str]:
    """List procedure refs that have parsed JSON files on S3."""
    prefix = f"{case}/procedures/parsed/"
    objects = s3.list_objects(prefix)
    return {
        obj["Key"].rsplit("/", 1)[-1].replace(".json", "")
        for obj in objects
        if obj["Key"].endswith(".json")
    }


def _run_parses(proc_refs: list[str], case: str, logger) -> None:
    """Run .map() parses, swallowing individual failures."""
    logger.info("Parsing %d procedures", len(proc_refs))
    futures = parse_one.map(proc_refs, unmapped(case))
    for future in futures:
        try:
            future.result()
        except Exception:
            pass


@flow(name="parse-eu", retries=0, task_runner=ProcessPoolTaskRunner(max_workers=16))
def parse_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
) -> None:
    """Parse all downloaded RDF files and write parsed JSON.

    After the initial batch, reconciles against S3 up to 3 times
    to retry any missing parses. Raises RuntimeError if the set
    is still incomplete.
    """
    if dry_run:
        get_run_logger().info("DRY RUN: skipping parsing")
        return

    logger = get_run_logger()
    versions = get_pipeline_versions()
    logger.info("Pipeline versions: %s", versions)

    proc_refs = list_parseable_refs(case, sample_mode, sample_limit)
    expected = set(proc_refs)

    # Initial run
    _run_parses(proc_refs, case, logger)

    # Reconciliation loop
    for round_num in range(1, MAX_RECONCILIATION_ROUNDS + 1):
        on_s3 = _list_parsed_refs(case)
        missing = expected - on_s3
        if not missing:
            logger.info("All %d procedures parsed successfully", len(expected))
            return

        logger.warning(
            "Reconciliation round %d: %d missing out of %d",
            round_num, len(missing), len(expected),
        )
        _run_parses(sorted(missing), case, logger)

    # Final check
    on_s3 = _list_parsed_refs(case)
    missing = expected - on_s3
    if missing:
        raise RuntimeError(
            f"Parsing incomplete after {MAX_RECONCILIATION_ROUNDS} reconciliation "
            f"rounds: {len(missing)} procedures still missing. "
            f"Examples: {sorted(missing)[:5]}"
        )
    logger.info("All %d procedures parsed successfully", len(expected))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="eu")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    parse_eu_flow(
        case=args.case,
        sample_mode=args.sample,
        sample_limit=args.sample_limit,
        dry_run=args.dry_run,
    )
