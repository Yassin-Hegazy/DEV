"""Shared test helpers: a throwaway project directory per test."""

import hashlib
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from nyc_housing_pipeline.config import PipelineConfig, load_config

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DATASET_KEY = "synthetic_permits"
SOURCE_FILENAME = "Synthetic_Permits_20260101.csv"
COLUMNS = ("PermitNumber", "SequenceNumber", "BoroughName", "IssueDate", "Comments", "Units")

# The exact records of fixtures/edge_cases.csv as Bronze must store them.
# None = unquoted empty field (null); "" = quoted empty field.
EDGE_CASE_ROWS: list[tuple[str | None, ...]] = [
    ("B01202600001", "0001", "BROOKLYN", "08/31/2026 03:11:54 PM", "Normal record", "12"),
    ("B01202600002", "0002", "Brooklyn ", "08/31/2026", "", None),
    ("B01202600003", None, "", "13/45/2026", "Invalid-looking date", "007"),
    ("0000123", "0010", "bk", "not a date", "Comma, inside quotes", "0"),
    ("B01202600005", "0001", "QUEENS", "2026-02-30", "Line one\nline two", "5"),
    ("B01202600006", "0001", "MANHATTAN", "", "Unicode: café – Bronx 🏙 日本", "  3 "),
    ("B01202600001", "0001", "BROOKLYN", "08/31/2026 03:11:54 PM", "Normal record", "12"),
    ("B01202600007", "0001", "STATEN ISLAND", "00/00/0000", 'Embedded "quotes" here', "-1"),
    (None, None, None, None, None, None),
]


def write_config(root: Path, datasets: dict[str, str], expected_columns=COLUMNS) -> Path:
    """Write a pipeline.toml under ``root``; ``datasets`` maps dataset key -> source glob."""
    columns = ", ".join(f'"{c}"' for c in expected_columns)
    lines = [
        '[paths]\nproject_root = ".."\nsource_dir = "data/source"',
        'bronze_dir = "data/bronze"\nlog_dir = "logs"',
        '[parquet]\ncompression = "zstd"\ncompression_level = 3',
        "[validation]\nsample_size = 1000\nsample_seed = 7",
    ]
    for key, pattern in datasets.items():
        lines.append(
            f'[datasets.{key}]\ntitle = "{key}"\nsocrata_id = "test-0000"\n'
            f'source_pattern = "{pattern}"\nexpected_columns = [{columns}]\n'
            'identifier_columns = ["PermitNumber"]'
        )
    path = root / "config" / "pipeline.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (root / "data" / "source").mkdir(parents=True, exist_ok=True)
    return path


@dataclass
class Sandbox:
    root: Path
    config: PipelineConfig

    def place_source(self, data: bytes | str) -> Path:
        """Put fixture file ``data`` (a name) or raw bytes in place as the source CSV."""
        target = self.config.source_dir / SOURCE_FILENAME
        if isinstance(data, str):
            shutil.copyfile(FIXTURES_DIR / data, target)
        else:
            target.write_bytes(data)
        return target


@pytest.fixture
def make_project(tmp_path: Path) -> Callable[..., Sandbox]:
    def make(fixture: str | None = "edge_cases.csv", expected_columns=COLUMNS) -> Sandbox:
        path = write_config(tmp_path, {DATASET_KEY: "Synthetic_Permits_*.csv"}, expected_columns)
        project = Sandbox(tmp_path, load_config(path))
        if fixture:
            project.place_source(fixture)
        return project

    return make


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
