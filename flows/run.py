"""Pipeline Orchestrator

CLI entry point that runs steps for one or more cases.

Usage:
    python -m flows.run --case eu --sample
    python -m flows.run --case eu --steps collect download
    python -m flows.run --case eu --download-mode incremental
    python -m flows.run --case all --sample --dry-run
"""

import argparse
import importlib
import os
from datetime import datetime, timezone

# Prevent Prefect's ephemeral event queue from saturating after high-volume
# steps (parse: ~135K events). Default is 10,000 which causes 503 errors.
os.environ.setdefault("PREFECT_EVENTS_MAXIMUM_SIZE_IN_QUEUE", "200000")

from prefect import flow, get_run_logger

from backstage.utils import s3

CASES = {
    "eu": "flows.eu",
}


def load_case(case_name: str) -> tuple[dict, list[str]]:
    """Import a case module and return its STEPS dict and DEFAULT_ORDER."""
    module_path = CASES[case_name]
    module = importlib.import_module(module_path)
    return module.get_steps(), module.DEFAULT_ORDER


@flow(name="run-pipeline", log_prints=True)
def run_pipeline(
    cases: list[str],
    steps: list[str] | None = None,
    sample_mode: bool = False,
    sample_limit: int = 10,
    dry_run: bool = False,
    download_mode: str = "full",
) -> dict:
    """Run pipeline steps for given cases.

    Args:
        cases: List of case names to run.
        steps: Specific steps to run (None = all steps in DEFAULT_ORDER).
        sample_mode: Limit data for testing.
        sample_limit: Number of items in sample mode.
        dry_run: Skip destructive operations (Dataverse publish).
        download_mode: "full" (re-download all) or "incremental" (only new).
    """
    logger = get_run_logger()
    start = datetime.now(timezone.utc)

    results = {}

    for case_name in cases:
        logger.info("Starting case: %s", case_name)
        available_steps, default_order = load_case(case_name)

        step_order = steps if steps else default_order

        for step_name in step_order:
            if step_name not in available_steps:
                logger.warning(
                    "Step '%s' not found in case '%s', skipping. Available: %s",
                    step_name, case_name, list(available_steps.keys()),
                )
                continue

            logger.info("Running %s/%s", case_name, step_name)
            step_fn = available_steps[step_name]
            kwargs = {
                "case": case_name,
                "sample_mode": sample_mode,
                "sample_limit": sample_limit,
                "dry_run": dry_run,
            }
            if step_name == "download":
                kwargs["mode"] = download_mode
            step_fn(**kwargs)
            logger.info("Completed %s/%s", case_name, step_name)

        results[case_name] = {"steps_run": step_order, "status": "complete"}

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()

    summary = {
        "cases": results,
        "sample_mode": sample_mode,
        "dry_run": dry_run,
        "elapsed_seconds": round(elapsed, 1),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("Pipeline complete in %.0fs", elapsed)

    for case_name in cases:
        try:
            s3.write_json(summary, f"{case_name}/state/last_run.json")
        except Exception as e:
            logger.warning("Failed to write run summary for %s: %s", case_name, e)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run backstage pipeline")
    parser.add_argument(
        "--case", default="eu",
        help="Case to run (eu, all). Default: eu",
    )
    parser.add_argument(
        "--steps", nargs="*", default=None,
        help="Specific steps to run. Default: all steps for the case.",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true", help="Skip Dataverse publish")
    parser.add_argument(
        "--download-mode",
        choices=["full", "incremental"],
        default="full",
        help="Download mode: full (re-download all) or incremental (only new). Default: full",
    )
    args = parser.parse_args()

    if args.case == "all":
        case_list = list(CASES.keys())
    else:
        case_list = [args.case]

    run_pipeline(
        cases=case_list,
        steps=args.steps,
        sample_mode=args.sample,
        sample_limit=args.sample_limit,
        dry_run=args.dry_run,
        download_mode=args.download_mode,
    )
