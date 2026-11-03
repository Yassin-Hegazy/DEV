"""Dagster assets for the Bronze layer: thin adapters around ``ingest_dataset``.

No ``from __future__ import annotations`` here: Dagster reads the ``context``
annotation at definition time.
"""

import dagster as dg

from nyc_housing_pipeline.bronze.ingestion import IngestionResult, ingest_dataset
from nyc_housing_pipeline.bronze.validation import CHECKS, CheckResult
from nyc_housing_pipeline.errors import PipelineError
from nyc_housing_pipeline.orchestration.resources import PipelineConfigResource

# Must match the dataset keys in config/pipeline.toml.
DATASETS = ("housing_production", "street_construction_permits")


def source_spec(dataset_key: str) -> dg.AssetSpec:
    """The manually downloaded CSV: shown in the lineage graph, never materialized."""
    return dg.AssetSpec(
        key=["source", dataset_key],
        group_name="source",
        kinds={"csv"},
        description=f"Source CSV for '{dataset_key}' in data/source/ (never modified).",
    )


def bronze_asset(dataset_key: str) -> dg.AssetsDefinition:
    key = dg.AssetKey(["bronze", dataset_key])

    @dg.asset(
        key=key,
        deps=[dg.AssetKey(["source", dataset_key])],
        group_name="bronze",
        kinds={"polars", "parquet"},
        description=f"All-string Parquet snapshot of the '{dataset_key}' source CSV.",
        check_specs=[
            dg.AssetCheckSpec(name, asset=key, description=description, blocking=blocking)
            for name, (description, blocking) in CHECKS.items()
        ],
    )
    def _asset(
        context: dg.AssetExecutionContext, pipeline: PipelineConfigResource
    ) -> dg.MaterializeResult:
        try:
            result = ingest_dataset(
                pipeline.load(),
                dataset_key,
                logger=context.log,
                orchestrator_run_id=context.run.run_id,
            )
        except PipelineError as exc:
            raise dg.Failure(description=f"{type(exc).__name__}: {exc}") from exc
        return dg.MaterializeResult(
            metadata=metadata(result),
            check_results=[check_result(c) for c in result.checks],
            data_version=dg.DataVersion(result.snapshot_id),
        )

    return _asset


def check_result(check: CheckResult) -> dg.AssetCheckResult:
    severity = dg.AssetCheckSeverity.ERROR if check.blocking else dg.AssetCheckSeverity.WARN
    return dg.AssetCheckResult(
        check_name=check.name, passed=check.passed, severity=severity, metadata=check.details
    )


def metadata(result: IngestionResult) -> dict[str, object]:
    source, output = result.manifest["source"], result.manifest["output"]
    ingestion = result.manifest["ingestion"]
    return {
        "status": result.status,
        "snapshot_id": result.snapshot_id,
        "dagster/row_count": output["row_count"],
        "source_path": dg.MetadataValue.path(source["path"]),
        "source_sha256": source["sha256"],
        "source_size_bytes": source["size_bytes"],
        "parquet_path": dg.MetadataValue.path(output["path"]),
        "parquet_size_bytes": output["size_bytes"],
        "compression_ratio": output["compression_ratio"],
        "duplicate_rows": result.manifest["diagnostics"]["duplicate_rows"],
        "ingested_at_utc": ingestion["finished_at_utc"],
        "timings_seconds": ingestion["timings_seconds"],
        "peak_memory_bytes": ingestion["peak_memory_bytes"],
    }


SOURCE_SPECS = [source_spec(key) for key in DATASETS]
BRONZE_ASSETS = [bronze_asset(key) for key in DATASETS]
