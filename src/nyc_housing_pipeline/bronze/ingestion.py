"""Source CSV -> Bronze snapshot for one dataset. Knows nothing about Dagster.

Steps:
 1. Find the source file, read its header, check it against the configured columns.
 2. Hash the source -> snapshot id. If that snapshot already exists and is intact, reuse it.
 3. Parse every record with the independent strict parser (count + sample).
 4. Stream CSV -> Parquet into a private staging directory.
 5. Validate. Any blocking failure: delete staging, raise, publish nothing.
 6. Write the manifest, rename staging -> snapshot (atomic), update latest.json.

Every attempt (created / reused / failed) is appended to logs/bronze_ingestion_runs.jsonl.
"""

import logging
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nyc_housing_pipeline.bronze import snapshots
from nyc_housing_pipeline.bronze.conversion import (
    CSV_READ_OPTIONS,
    scan_source_csv,
    settings_fingerprint,
    write_parquet,
)
from nyc_housing_pipeline.bronze.source import find_source, read_header, scan_records
from nyc_housing_pipeline.bronze.validation import (
    CheckResult,
    check_contract,
    check_output,
    profile_output,
)
from nyc_housing_pipeline.config import DatasetConfig, PipelineConfig
from nyc_housing_pipeline.errors import SourceError, ValidationError
from nyc_housing_pipeline.fileio import append_jsonl, file_state, sha256_file, write_json
from nyc_housing_pipeline.runtime import environment_info, peak_memory_bytes, timed, utc_now

RUN_LOG_FILE = "bronze_ingestion_runs.jsonl"

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionResult:
    status: str  # "created" or "reused"
    snapshot_dir: Path
    manifest: dict[str, Any]
    checks: list[CheckResult]

    @property
    def snapshot_id(self) -> str:
        return self.manifest["snapshot_id"]

    @property
    def parquet_path(self) -> Path:
        return self.snapshot_dir / snapshots.PARQUET_FILE

    @property
    def manifest_path(self) -> Path:
        return self.snapshot_dir / snapshots.MANIFEST_FILE


def ingest_dataset(
    config: PipelineConfig,
    dataset_key: str,
    *,
    logger: logging.Logger | None = None,
    orchestrator_run_id: str | None = None,
) -> IngestionResult:
    """Ingest one dataset and log the attempt, whatever its outcome."""
    log_entry: dict[str, Any] = {
        "dataset": dataset_key,
        "orchestrator_run_id": orchestrator_run_id,
        "started_at_utc": utc_now(),
    }
    start = time.perf_counter()
    try:
        result = _ingest(config, config.dataset(dataset_key), logger or _LOGGER, log_entry)
        log_entry |= {"status": result.status, "snapshot_id": result.snapshot_id}
        return result
    except Exception as exc:
        log_entry |= {"status": "failed", "error_type": type(exc).__name__, "error": str(exc)}
        raise
    finally:
        log_entry["duration_seconds"] = round(time.perf_counter() - start, 3)
        append_jsonl(config.log_dir / RUN_LOG_FILE, log_entry)


def _ingest(
    config: PipelineConfig, dataset: DatasetConfig, log: logging.Logger, log_entry: dict[str, Any]
) -> IngestionResult:
    start = time.perf_counter()
    timings: dict[str, float] = {}

    # 1. Source file and header ------------------------------------------------
    with timed(timings, "read_header"):
        source_path = find_source(config.source_dir, dataset.source_pattern)
        source_state = file_state(source_path)
        columns = read_header(source_path)
    contract = check_contract(columns, dataset.expected_columns)
    if contract.details["missing"]:
        raise SourceError(
            f"{source_path.name} is missing expected columns {contract.details['missing']}"
        )
    log.info(
        "[%s] source %s: %s bytes, %d columns",
        dataset.key,
        source_path.name,
        f"{source_state[0]:,}",
        len(columns),
    )

    # 2. Identity and reuse --------------------------------------------------
    with timed(timings, "hash_source"):
        source_sha256 = sha256_file(source_path)
    fingerprint = settings_fingerprint(config.parquet)
    snapshot = snapshots.snapshot_id(source_sha256, fingerprint)
    final_dir = snapshots.snapshot_dir(config.bronze_dir, dataset.key, snapshot)

    existing = snapshots.load_snapshot(final_dir)
    if existing:
        log.info("[%s] snapshot %s already exists and is intact: reusing", dataset.key, snapshot)
        _point_latest(config, dataset.key, final_dir, existing)
        checks = [CheckResult(**c) for c in existing["checks"]]
        return IngestionResult("reused", final_dir, existing, checks)

    # 3. Independent parse ---------------------------------------------------
    with timed(timings, "verify_source_records"):
        scan = scan_records(source_path, len(columns), config.sample_size, config.sample_seed)
    log.info("[%s] %s records verified", dataset.key, f"{scan.record_count:,}")

    run_id = uuid.uuid4().hex
    staging = snapshots.staging_dir(config.bronze_dir, dataset.key, run_id)
    staging.mkdir(parents=True)
    try:
        # 4. Conversion --------------------------------------------------------
        parquet = staging / snapshots.PARQUET_FILE
        with timed(timings, "csv_to_parquet"):
            write_parquet(
                scan_source_csv(source_path),
                parquet,
                config.parquet,
                {"source_sha256": source_sha256, "snapshot_id": snapshot},
            )
        log.info("[%s] CSV -> Parquet in %.2fs", dataset.key, timings["csv_to_parquet"])

        # 5. Validation ----------------------------------------------------------
        with timed(timings, "validate_output"):
            checks = [
                CheckResult(
                    "source_unchanged",
                    file_state(source_path) == source_state,
                    {"size_bytes": source_state[0]},
                ),
                *check_output(parquet, columns, scan.record_count, scan.sample),
            ]
        failed = [c for c in checks if c.failed_blocking]
        if failed:
            raise ValidationError(
                f"[{dataset.key}] validation failed, nothing published: "
                + "; ".join(f"{c.name} {c.details}" for c in failed)
            )
        with timed(timings, "profile_output"):
            diagnostics, warnings = profile_output(parquet, dataset.identifier_columns)
        checks += [contract, *warnings]
        for check in checks:
            if not check.passed:
                log.warning("[%s] %s: %s", dataset.key, check.name, check.details)

        # 6. Manifest and publication -----------------------------------------
        output_size = parquet.stat().st_size
        manifest = {
            "snapshot_id": snapshot,
            "dataset": {
                "key": dataset.key,
                "title": dataset.title,
                "socrata_id": dataset.socrata_id,
            },
            "source": {
                "filename": source_path.name,
                "path": config.relative(source_path),
                "size_bytes": source_state[0],
                "sha256": source_sha256,
                "record_count": scan.record_count,
                "physical_lines": scan.physical_lines,
                "column_count": len(columns),
                "columns": list(columns),
            },
            "output": {
                "path": config.relative(final_dir / snapshots.PARQUET_FILE),
                "size_bytes": output_size,
                "sha256": sha256_file(parquet),
                "row_count": diagnostics["rows"],
                "column_count": len(columns),
                "dtype": "String (every column)",
                "compression_ratio": round(source_state[0] / output_size, 2),
            },
            "settings": {"csv_read_options": CSV_READ_OPTIONS, "fingerprint": fingerprint},
            "ingestion": {
                "run_id": run_id,
                "orchestrator_run_id": log_entry["orchestrator_run_id"],
                "started_at_utc": log_entry["started_at_utc"],
                "finished_at_utc": utc_now(),
                "duration_seconds": round(time.perf_counter() - start, 3),
                "timings_seconds": timings,
                "peak_memory_bytes": peak_memory_bytes(),
            },
            "checks": [c.to_dict() for c in checks],
            "diagnostics": diagnostics,
            "environment": environment_info(),
        }
        write_json(staging / snapshots.MANIFEST_FILE, manifest)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(final_dir)  # atomic publish
    finally:
        shutil.rmtree(staging, ignore_errors=True)  # no-op after a successful rename

    _point_latest(config, dataset.key, final_dir, manifest)
    log.info("[%s] published snapshot %s", dataset.key, snapshot)
    return IngestionResult("created", final_dir, manifest, checks)


def _point_latest(
    config: PipelineConfig, dataset_key: str, directory: Path, manifest: dict[str, Any]
) -> None:
    snapshots.write_latest(
        config.bronze_dir,
        dataset_key,
        {
            "snapshot_id": manifest["snapshot_id"],
            "parquet_path": config.relative(directory / snapshots.PARQUET_FILE),
            "manifest_path": config.relative(directory / snapshots.MANIFEST_FILE),
            "source_sha256": manifest["source"]["sha256"],
            "updated_at_utc": utc_now(),
        },
    )
