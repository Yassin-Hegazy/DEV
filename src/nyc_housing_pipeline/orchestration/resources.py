"""Dagster resources."""

import dagster as dg

from nyc_housing_pipeline.config import PipelineConfig, load_config


class PipelineConfigResource(dg.ConfigurableResource):
    """Gives assets the pipeline config (default: $NYC_PIPELINE_CONFIG or config/pipeline.toml)."""

    config_path: str | None = None

    def load(self) -> PipelineConfig:
        return load_config(self.config_path)
