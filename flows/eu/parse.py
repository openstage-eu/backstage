"""EU Procedure Parsing Pipeline

Reads downloaded RDF files from S3 at {case}/procedures/raw/{proc_ref}.rdf,
parses them with openbasement, converts to openstage EUProcedure models via
the adapter, and writes model_dump() output to {case}/procedures/parsed/{proc_ref}.json.

Always parses every RDF file. Monthly full rebuild makes skip logic
unnecessary. Records dependency commit SHAs for reproducibility.
"""

from datetime import datetime, timezone
from io import BytesIO

from prefect import flow, task, get_run_logger
from prefect.task_runners import ProcessPoolTaskRunner
from prefect.utilities.annotations import unmapped

from backstage.utils import s3
from backstage.utils.versions import get_pipeline_versions


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
    meta_key = f"{case}/procedures/meta/{proc_ref}.json"
    parsed_key = f"{case}/procedures/parsed/{proc_ref}.json"

    rdf_bytes = s3.read_bytes(rdf_key)
    graph = Graph()
    graph.parse(BytesIO(rdf_bytes), format="xml")
    result = extract(graph, template="eu_procedure")

    procedure = EUProcedure.from_openbasement(result[0])
    s3.write_json(procedure.model_dump(), parsed_key)

    meta = s3.read_json(meta_key)
    meta["parsed_at"] = datetime.now(timezone.utc).isoformat()
    s3.write_json(meta, meta_key)

    return {"proc_ref": proc_ref, "status": "parsed"}


@task
def aggregate_parse_results(results: list[dict]) -> dict:
    """Aggregate results from parallel parse tasks."""
    logger = get_run_logger()
    stats = {
        "total": len(results),
        "parsed": sum(1 for r in results if r["status"] == "parsed"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
    }
    logger.info("Parsing complete: %s", stats)
    return stats


@task
def validate_parsing(stats: dict) -> None:
    """Check failure rate is acceptable."""
    logger = get_run_logger()
    total = stats["total"]
    if total > 0 and stats["failed"] / total > 0.1:
        raise ValueError(
            f"High parse failure rate: {stats['failed']}/{total} "
            f"({stats['failed']/total:.1%})"
        )
    logger.info("Parse validation passed")


@flow(name="parse-eu", retries=0, task_runner=ProcessPoolTaskRunner(max_workers=16))
def parse_eu_flow(
    case: str = "eu",
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
) -> None:
    """Parse all downloaded RDF files and write parsed JSON."""
    if dry_run:
        get_run_logger().info("DRY RUN: skipping parsing")
        return

    logger = get_run_logger()
    versions = get_pipeline_versions()
    logger.info("Pipeline versions: %s", versions)

    proc_refs = list_parseable_refs(case, sample_mode, sample_limit)

    # Process in batches to avoid overwhelming Prefect's ephemeral server
    batch_size = 500
    results = []
    for i in range(0, len(proc_refs), batch_size):
        batch = proc_refs[i:i + batch_size]
        logger.info(
            "Parse batch %d-%d / %d",
            i + 1, min(i + batch_size, len(proc_refs)), len(proc_refs),
        )
        futures = parse_one.map(batch, unmapped(case))
        for future in futures:
            try:
                results.append(future.result())
            except Exception:
                results.append({"proc_ref": "unknown", "status": "failed"})

    stats = aggregate_parse_results(results)
    validate_parsing(stats)


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
