"""Load ``config/pipeline.toml`` into plain dataclasses."""

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from nyc_housing_pipeline.errors import ConfigError

CONFIG_ENV_VAR = "NYC_PIPELINE_CONFIG"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "pipeline.toml"


@dataclass(frozen=True)
class ParquetSettings:
    compression: str
    compression_level: int | None
    row_group_size: int | None


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    title: str
    socrata_id: str
    source_pattern: str
    expected_columns: tuple[str, ...]
    identifier_columns: tuple[str, ...]


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    source_dir: Path
    bronze_dir: Path
    log_dir: Path
    parquet: ParquetSettings
    sample_size: int
    sample_seed: int
    datasets: dict[str, DatasetConfig]

    def dataset(self, key: str) -> DatasetConfig:
        if key not in self.datasets:
            raise ConfigError(f"Dataset '{key}' is not configured (known: {list(self.datasets)})")
        return self.datasets[key]

    def relative(self, path: Path) -> str:
        """``path`` relative to the project root, in POSIX form (absolute if outside it)."""
        path = path.resolve()
        if path.is_relative_to(self.project_root):
            return path.relative_to(self.project_root).as_posix()
        return path.as_posix()


def load_config(path: str | Path | None = None) -> PipelineConfig:
    config_path = Path(path or os.environ.get(CONFIG_ENV_VAR) or DEFAULT_CONFIG_PATH).resolve()
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
        paths = raw["paths"]
        root = (config_path.parent / paths["project_root"]).resolve()
        return PipelineConfig(
            project_root=root,
            source_dir=root / paths["source_dir"],
            bronze_dir=root / paths["bronze_dir"],
            log_dir=root / paths["log_dir"],
            parquet=ParquetSettings(
                compression=raw["parquet"]["compression"],
                compression_level=raw["parquet"].get("compression_level"),
                row_group_size=raw["parquet"].get("row_group_size"),
            ),
            sample_size=raw["validation"]["sample_size"],
            sample_seed=raw["validation"]["sample_seed"],
            datasets={
                key: DatasetConfig(
                    key=key,
                    title=table["title"],
                    socrata_id=table["socrata_id"],
                    source_pattern=table["source_pattern"],
                    expected_columns=tuple(table["expected_columns"]),
                    identifier_columns=tuple(table.get("identifier_columns", [])),
                )
                for key, table in raw["datasets"].items()
            },
        )
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"Invalid pipeline config {config_path}: {exc!r}") from exc
