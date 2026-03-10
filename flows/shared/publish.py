"""Shared Dataverse Publishing

Publishes packaged datasets to Harvard Dataverse with proper versioning
and metadata. Reads the packaged ZIP from S3, uploads to Dataverse.

Dataverse API is the source of truth for publication state.
Writes state/publication.json to S3 for audit.
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from prefect import task, get_run_logger
from prefect.tasks import exponential_backoff

from backstage.dataset_config import make_dataset_config
from backstage.utils import s3


@task
def find_latest_package(case: str, package_date: str = "") -> dict:
    """Find the latest packaged dataset in S3."""
    logger = get_run_logger()

    if package_date:
        logger.info("Looking for package from date: %s", package_date)
        prefix = f"{case}/datasets/{package_date}/"
    else:
        logger.info("Scanning for latest package...")
        all_objects = s3.list_objects(f"{case}/datasets/")

        if not all_objects:
            raise ValueError(f"No packages found in {case}/datasets/")

        dates = set()
        for obj in all_objects:
            parts = obj["Key"].split("/")
            if len(parts) >= 3:
                dates.add(parts[2])

        if not dates:
            raise ValueError("No dated packages found")

        package_date = max(dates)
        prefix = f"{case}/datasets/{package_date}/"
        logger.info("Latest package date: %s", package_date)

    objects = s3.list_objects(prefix)

    zip_file = None
    metadata_file = None

    for obj in objects:
        key = obj["Key"]
        if key.endswith(".zip"):
            zip_file = key
        elif key.endswith("metadata.json"):
            metadata_file = key

    if not zip_file:
        raise ValueError(f"No ZIP file found for date {package_date}")
    if not metadata_file:
        raise ValueError(f"No metadata file found for date {package_date}")

    metadata = s3.read_json(metadata_file)

    package_info = {
        "package_date": package_date,
        "zip_s3_key": zip_file,
        "metadata_s3_key": metadata_file,
        "metadata": metadata,
        "zip_filename": zip_file.split("/")[-1],
    }

    logger.info("Found package: %s (%d procedures)",
                package_info["zip_filename"],
                metadata.get("total_procedures", 0))
    return package_info


@task
def prepare_dataverse_metadata(package_info: dict, config: dict) -> dict:
    """Prepare metadata in Dataverse format using per-case config."""
    logger = get_run_logger()

    metadata = package_info["metadata"]
    run_date = metadata["creation_date"]
    dataset_title = config["title_template"].format(date=run_date)

    contact_email = os.environ.get("DATAVERSE_CONTACT_EMAIL", "contact@example.com")
    contact_name = os.environ.get("DATAVERSE_CONTACT_NAME", "openstage Team")
    depositor = os.environ.get("DATAVERSE_DEPOSITOR", "openstage Pipeline")

    dataverse_metadata = {
        "datasetVersion": {
            "metadataBlocks": {
                "citation": {
                    "fields": [
                        {
                            "typeName": "title",
                            "value": dataset_title,
                        },
                        {
                            "typeName": "author",
                            "value": [
                                {
                                    "authorName": {
                                        "typeName": "authorName",
                                        "value": config["author_name"],
                                    },
                                    "authorAffiliation": {
                                        "typeName": "authorAffiliation",
                                        "value": config["author_affiliation"],
                                    },
                                }
                            ],
                        },
                        {
                            "typeName": "datasetContact",
                            "value": [
                                {
                                    "datasetContactEmail": {
                                        "typeName": "datasetContactEmail",
                                        "value": contact_email,
                                    },
                                    "datasetContactName": {
                                        "typeName": "datasetContactName",
                                        "value": contact_name,
                                    },
                                }
                            ],
                        },
                        {
                            "typeName": "dsDescription",
                            "value": [
                                {
                                    "dsDescriptionValue": {
                                        "typeName": "dsDescriptionValue",
                                        "value": (
                                            f"{config['description']}. "
                                            f"Contains {metadata['total_procedures']} "
                                            f"parsed procedures. "
                                            f"Generated on {run_date}."
                                        ),
                                    }
                                }
                            ],
                        },
                        {
                            "typeName": "subject",
                            "value": config["subject"],
                        },
                        {
                            "typeName": "keyword",
                            "value": [
                                {"keywordValue": {"typeName": "keywordValue", "value": kw}}
                                for kw in config["keywords"]
                            ],
                        },
                        {
                            "typeName": "depositor",
                            "value": depositor,
                        },
                        {
                            "typeName": "dateOfDeposit",
                            "value": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        },
                    ]
                },
                "geospatial": {
                    "fields": [
                        {
                            "typeName": "geographicCoverage",
                            "value": [
                                {
                                    "country": {
                                        "typeName": "country",
                                        "value": config["geographic_coverage"],
                                    }
                                }
                            ],
                        }
                    ]
                },
            }
        },
    }

    logger.info("Dataverse metadata prepared for: %s", dataset_title)
    return dataverse_metadata


@task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def upload_to_dataverse(
    package_info: dict,
    dataverse_metadata: dict,
    case: str,
    dry_run: bool = False,
    create_new_version: bool = True,
    visibility: str = "public",
) -> dict:
    """Upload dataset to Harvard Dataverse."""
    logger = get_run_logger()

    import requests

    dataverse_server = os.environ.get("DATAVERSE_SERVER_URL", "")
    dataverse_api_token = os.environ.get("DATAVERSE_API_TOKEN", "")
    dataverse_alias = os.environ.get("DATAVERSE_ALIAS", "openstage")
    case_key = case.upper()
    dataset_persistent_id = os.environ.get(
        f"DATASET_PERSISTENT_ID_{case_key}",
        os.environ.get("DATASET_PERSISTENT_ID", ""),
    )

    if dry_run:
        logger.info("DRY RUN - Would publish to Dataverse:")
        logger.info("  Server: %s", dataverse_server)
        logger.info("  Dataverse: %s", dataverse_alias)
        logger.info("  Package: %s", package_info["zip_filename"])
        return {"status": "dry_run", "would_publish": True}

    if not dataverse_api_token:
        raise ValueError(
            "Dataverse API token not configured. "
            "Set DATAVERSE_API_TOKEN environment variable."
        )

    logger.info("Publishing to Dataverse: %s", dataverse_server)

    with tempfile.TemporaryDirectory() as temp_dir:
        local_zip_path = Path(temp_dir) / package_info["zip_filename"]
        s3.download(package_info["zip_s3_key"], str(local_zip_path))

        headers = {
            "X-Dataverse-key": dataverse_api_token,
            "Content-Type": "application/json",
        }

        if dataset_persistent_id and create_new_version:
            logger.info("Creating new version of dataset: %s", dataset_persistent_id)

            metadata_url = (
                f"{dataverse_server}/api/datasets/:persistentId/versions/:draft"
            )
            params = {"persistentId": dataset_persistent_id}

            metadata_response = requests.put(
                metadata_url,
                params=params,
                headers=headers,
                data=json.dumps(dataverse_metadata),
                timeout=300,
            )

            if metadata_response.status_code not in [200, 201]:
                raise ValueError(
                    f"Metadata update failed: {metadata_response.status_code} "
                    f"{metadata_response.text}"
                )

            file_url = f"{dataverse_server}/api/datasets/:persistentId/add"
            with open(local_zip_path, "rb") as zip_fh:
                files = {
                    "file": zip_fh,
                    "jsonData": (
                        None,
                        json.dumps({
                            "description": (
                                f"openstage Procedures Dataset - "
                                f"{package_info['package_date']}"
                            ),
                            "categories": ["Data"],
                            "restrict": False,
                        }),
                    ),
                }
                upload_response = requests.post(
                    file_url,
                    params=params,
                    headers={"X-Dataverse-key": dataverse_api_token},
                    files=files,
                    timeout=1800,
                )

            if upload_response.status_code not in [200, 201]:
                raise ValueError(
                    f"File upload failed: {upload_response.status_code} "
                    f"{upload_response.text}"
                )

            if visibility == "public":
                publish_url = (
                    f"{dataverse_server}/api/datasets/"
                    f":persistentId/actions/:publish"
                )
                publish_response = requests.post(
                    publish_url,
                    params={**params, "type": "major"},
                    headers={"X-Dataverse-key": dataverse_api_token},
                    timeout=300,
                )
                if publish_response.status_code not in [200, 201]:
                    logger.warning(
                        "Failed to publish (saved as draft): %s",
                        publish_response.text,
                    )
                else:
                    logger.info("Dataset version published successfully")

            action = "new_version"

        else:
            logger.info("Creating new dataset in dataverse: %s", dataverse_alias)

            create_url = (
                f"{dataverse_server}/api/dataverses/{dataverse_alias}/datasets"
            )
            create_response = requests.post(
                create_url,
                headers=headers,
                data=json.dumps(dataverse_metadata),
                timeout=300,
            )

            if create_response.status_code not in [200, 201]:
                raise ValueError(
                    f"Dataset creation failed: {create_response.status_code} "
                    f"{create_response.text}"
                )

            dataset_info = create_response.json()
            dataset_persistent_id = dataset_info["data"]["persistentId"]

            logger.info("Dataset created with ID: %s", dataset_persistent_id)

            file_url = f"{dataverse_server}/api/datasets/:persistentId/add"
            with open(local_zip_path, "rb") as zip_fh:
                files = {
                    "file": zip_fh,
                    "jsonData": (
                        None,
                        json.dumps({
                            "description": (
                                f"openstage Procedures Dataset - "
                                f"{package_info['package_date']}"
                            ),
                            "categories": ["Data"],
                            "restrict": False,
                        }),
                    ),
                }
                upload_response = requests.post(
                    file_url,
                    params={"persistentId": dataset_persistent_id},
                    headers={"X-Dataverse-key": dataverse_api_token},
                    files=files,
                    timeout=1800,
                )

            if upload_response.status_code not in [200, 201]:
                raise ValueError(
                    f"File upload failed: {upload_response.status_code} "
                    f"{upload_response.text}"
                )

            if visibility == "public":
                publish_url = (
                    f"{dataverse_server}/api/datasets/"
                    f":persistentId/actions/:publish"
                )
                publish_response = requests.post(
                    publish_url,
                    params={"persistentId": dataset_persistent_id, "type": "major"},
                    headers={"X-Dataverse-key": dataverse_api_token},
                    timeout=300,
                )
                if publish_response.status_code not in [200, 201]:
                    logger.warning(
                        "Failed to publish (saved as draft): %s",
                        publish_response.text,
                    )
                else:
                    logger.info("Dataset published successfully")

            action = "new_dataset"

        publication_result = {
            "status": "success",
            "action": action,
            "dataset_persistent_id": dataset_persistent_id,
            "dataverse_server": dataverse_server,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "visibility": visibility,
            "package_date": package_info["package_date"],
            "total_procedures": package_info["metadata"]["total_procedures"],
        }

        pub_key = f"{case}/state/publication.json"
        s3.write_json(publication_result, pub_key)

        logger.info("Published to Dataverse: %s", dataset_persistent_id)
        return publication_result


def publish_dataset(
    case: str,
    geographic_coverage: str,
    subject: list[str],
    keywords: list[str],
    author_affiliation: str = "Research Institution",
    dry_run: bool = False,
    package_date: str = "",
    create_new_version: bool = True,
    visibility: str = "public",
) -> dict:
    """Top-level entry point for publishing. Called by case wrappers.

    Builds dataset config from case name and case-specific metadata,
    then finds the latest package and publishes to Dataverse.
    """
    config = make_dataset_config(
        case=case,
        geographic_coverage=geographic_coverage,
        subject=subject,
        keywords=keywords,
        author_affiliation=author_affiliation,
    )
    package_info = find_latest_package(case=case, package_date=package_date)
    dataverse_metadata = prepare_dataverse_metadata(
        package_info=package_info,
        config=config,
    )
    return upload_to_dataverse(
        package_info=package_info,
        dataverse_metadata=dataverse_metadata,
        case=case,
        dry_run=dry_run,
        create_new_version=create_new_version,
        visibility=visibility,
    )
