"""Shared Dataset Packaging

Reads parsed procedure JSON from {case}/procedures/parsed/ and packages
them into a ZIP dataset at {case}/datasets/{release}/ using openstage's
Dataset.dump().
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from prefect import task, get_run_logger

from backstage.utils import s3


@task(retries=2, retry_delay_seconds=10)
def load_parsed_procedures(case: str) -> list[dict]:
    """Load all parsed procedure JSON files from S3.

    Tracks per-file failures and raises RuntimeError if any file
    fails to load (task-level retries handle transient errors first).
    """
    logger = get_run_logger()

    prefix = f"{case}/procedures/parsed/"
    objects = s3.list_objects(prefix)
    json_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".json")]

    procedures = []
    failed_keys = []
    for key in json_keys:
        try:
            proc = s3.read_json(key)
            procedures.append(proc)
        except Exception as e:
            logger.error("Failed to load %s: %s", key, e)
            failed_keys.append(key)

    if failed_keys:
        raise RuntimeError(
            f"Failed to load {len(failed_keys)} parsed files: "
            f"{failed_keys[:5]}"
        )

    logger.info("Loaded %d parsed procedures", len(procedures))
    return procedures


@task
def build_package(
    procedures: list[dict],
    case: str,
    release: str = "",
    dataset_name: str = "",
    dataset_label: str = "",
    pipeline_versions: dict | None = None,
) -> dict:
    """Build ZIP dataset from parsed procedures using openstage Dataset.

    Cross-checks loaded procedure count against raw RDF file count on S3.
    Mismatch raises RuntimeError (indicates parse step missed something).

    Args:
        release: Release period label (YYYY.MM). Defaults to previous month.
        dataset_name: Dataset identifier for the registry (e.g. "openstage-eu").
        dataset_label: Human-readable label (e.g. "EU Procedures").
        pipeline_versions: Dependency commit SHAs for reproducibility.
    """
    from openstage.dataset import Dataset, resolve_class
    from openstage.models.procedure import Procedure

    logger = get_run_logger()

    # Cross-check: parsed count must equal raw RDF count
    raw_prefix = f"{case}/procedures/raw/"
    raw_objects = s3.list_objects(raw_prefix)
    raw_count = sum(1 for obj in raw_objects if obj["Key"].endswith(".rdf"))
    parsed_count = len(procedures)

    if parsed_count != raw_count:
        raise RuntimeError(
            f"Procedure count mismatch: {parsed_count} parsed files vs "
            f"{raw_count} raw RDF files. Parse step may be incomplete."
        )

    logger.info(
        "Cross-check passed: %d parsed == %d raw", parsed_count, raw_count,
    )

    creation_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not release:
        now = datetime.now(timezone.utc)
        if now.month == 1:
            release = f"{now.year - 1}.12"
        else:
            release = f"{now.year}.{now.month - 1:02d}"

    if not dataset_name:
        dataset_name = f"openstage-{case}"

    if not dataset_label:
        dataset_label = f"{case.upper()} Procedures"

    procedure_class = resolve_class(dataset_name) or Procedure
    typed_procedures = [procedure_class.model_validate(d) for d in procedures]

    description = (
        f"openstage {dataset_label} Dataset. "
        f"Contains {len(typed_procedures)} parsed legislative procedures."
    )

    dataset = Dataset(
        typed_procedures,
        name=dataset_name,
        version=release,
        description=description,
        creation_date=creation_date,
        pipeline_versions=pipeline_versions,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        zip_filename = f"openstage-{case}-{release}.zip"
        zip_path = temp_path / zip_filename

        dataset.dump(zip_path, format="individual")

        zip_key = f"{case}/datasets/{release}/{zip_filename}"
        s3.upload(str(zip_path), zip_key)

        metadata = {
            "name": dataset_name,
            "version": release,
            "description": description,
            "creation_date": creation_date,
            "total_procedures": len(typed_procedures),
        }
        if pipeline_versions:
            metadata["pipeline_versions"] = pipeline_versions

        metadata_key = f"{case}/datasets/{release}/metadata.json"
        s3.write_json(metadata, metadata_key)

        size_mb = zip_path.stat().st_size / (1024 * 1024)
        logger.info(
            "Package created: %s (%.1f MB, %d procedures)",
            zip_filename, size_mb, len(typed_procedures),
        )

        return {
            "zip_key": zip_key,
            "metadata_key": metadata_key,
            "size_mb": round(size_mb, 2),
            "total_procedures": len(typed_procedures),
            "release": release,
        }


def build_dataset_package(
    case: str,
    release: str = "",
    dataset_name: str = "",
    dataset_label: str = "",
    pipeline_versions: dict | None = None,
) -> dict:
    """Top-level entry point for packaging. Called by case wrappers."""
    procedures = load_parsed_procedures(case)
    return build_package(
        procedures,
        case,
        release=release,
        dataset_name=dataset_name,
        dataset_label=dataset_label,
        pipeline_versions=pipeline_versions,
    )
