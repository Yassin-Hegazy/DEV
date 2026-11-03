"""Dagster code location. ``dagster dev`` finds it via [tool.dagster] in pyproject.toml."""

import dagster as dg

from nyc_housing_pipeline.orchestration.bronze_assets import BRONZE_ASSETS, SOURCE_SPECS
from nyc_housing_pipeline.orchestration.resources import PipelineConfigResource

defs = dg.Definitions(
    assets=[*SOURCE_SPECS, *BRONZE_ASSETS],
    jobs=[
        dg.define_asset_job(
            "bronze_ingestion_all",
            selection=dg.AssetSelection.groups("bronze"),
            description="Ingest every source dataset into the Bronze layer.",
        )
    ],
    resources={"pipeline": PipelineConfigResource()},
)
