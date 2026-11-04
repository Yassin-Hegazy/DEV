"""Housing source migration: building-level dataset hg8x-zxpr replaces project-level hq68-rnsi."""

import csv
import shutil
from pathlib import Path

import polars as pl
import pytest

from nyc_housing_pipeline.bronze.ingestion import ingest_dataset
from nyc_housing_pipeline.bronze.snapshots import read_latest
from nyc_housing_pipeline.bronze.source import find_source
from nyc_housing_pipeline.config import DEFAULT_CONFIG_PATH, load_config
from nyc_housing_pipeline.errors import SourceError
from tests.conftest import FIXTURES_DIR, write_config

KEY = "housing_production"
FIXTURE = FIXTURES_DIR / "housing_buildings.csv"
BUILDING_FILE = "Affordable_Housing_Production_by_Building_20260101.csv"
PROJECT_FILE = "Affordable_Housing_Production_by_Project_20260101.csv"
REAL = load_config(DEFAULT_CONFIG_PATH)
HOUSING = REAL.dataset(KEY)

# Header of the retired project-level dataset (hq68-rnsi), for migration scenarios.
PROJECT_COLUMNS = (
    "Project ID", "Project Name", "Program Group", "Project Start Date",
    "Project Completion Date", "Extended Affordability Only", "Prevailing Wage Status",
    "Planned Tax Benefit", "Extremely Low Income Units", "Very Low Income Units",
    "Low Income Units", "Moderate Income Units", "Middle Income Units", "Other Income Units",
    "Counted Rental Units", "Counted Homeownership Units", "All Counted Units", "Total Units",
    "Senior Units",
)  # fmt: skip


def read_csv(path: Path) -> list[list[str | None]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle, quoting=csv.QUOTE_NOTNULL, strict=True))


def write_csv(path: Path, rows: list[list[str | None]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv.writer(handle, quoting=csv.QUOTE_NOTNULL, lineterminator="\n").writerows(rows)


def housing_config(root: Path, columns=HOUSING.expected_columns, pattern=HOUSING.source_pattern):
    """A sandbox config whose housing entry mirrors the real one."""
    path = write_config(
        root,
        {KEY: pattern},
        columns,
        socrata_id=HOUSING.socrata_id,
        identifier_columns=HOUSING.identifier_columns,
    )
    return load_config(path)


# --- the active configuration --------------------------------------------------


def test_active_housing_source_is_building_level() -> None:
    assert HOUSING.socrata_id == "hg8x-zxpr"
    assert "by_Building" in HOUSING.source_pattern
    assert {"Building ID", "Borough", "Postcode", "BBL", "BIN", "Latitude", "Longitude"} <= set(
        HOUSING.expected_columns
    )


def test_project_id_is_not_treated_as_unique() -> None:
    """Project ID repeats once per building, so it must not be an identifier column."""
    assert "Project ID" not in HOUSING.identifier_columns


def test_transportation_configuration_is_unchanged() -> None:
    permits = REAL.dataset("street_construction_permits")
    assert permits.socrata_id == "tqtj-sjs8"
    assert permits.source_pattern == "Street_Construction_Permits_(2022_-_Present)_*.csv"
    assert len(permits.expected_columns) == 39
    assert permits.expected_columns[0] == "PermitNumber"
    assert permits.identifier_columns == ("PermitNumber",)


def test_building_file_is_selected_when_both_housing_files_exist(tmp_path: Path) -> None:
    (tmp_path / PROJECT_FILE).write_text("x\n")
    (tmp_path / BUILDING_FILE).write_text("x\n")
    assert find_source(tmp_path, HOUSING.source_pattern).name == BUILDING_FILE


# --- ingestion of building-level data ---------------------------------------------


@pytest.fixture
def config(tmp_path: Path):
    config = housing_config(tmp_path)
    shutil.copyfile(FIXTURE, config.source_dir / BUILDING_FILE)
    (config.source_dir / PROJECT_FILE).write_text(",".join(PROJECT_COLUMNS) + "\n")
    return config


def test_every_building_row_is_preserved_exactly(config) -> None:
    result = ingest_dataset(config, KEY)
    header, *rows = read_csv(FIXTURE)
    frame = pl.read_parquet(result.parquet_path)

    assert frame.columns == header
    assert frame.rows() == [tuple(row) for row in rows]
    assert frame["Project ID"].to_list().count("90001") == 3  # repeated, not deduplicated
    assert frame["Building ID"].to_list().count("500001") == 2  # same building, two projects
    assert frame["Building ID"][2] is None  # not geocoded: kept as null
    assert (frame["Postcode"][3], frame["Census Tract"][0]) == ("01234", "000100")
    assert (frame["BBL"][0], frame["BIN"][3]) == ("3000010001", "3000000")
    assert frame["Project Completion Date"][4] == ""  # quoted empty stays ""
    assert not any(c.failed_blocking for c in result.checks)


def test_manifest_identifies_the_building_dataset(config) -> None:
    result = ingest_dataset(config, KEY)
    manifest = result.manifest
    assert (manifest["dataset"]["key"], manifest["dataset"]["socrata_id"]) == (KEY, "hg8x-zxpr")
    assert manifest["source"]["filename"] == BUILDING_FILE
    assert manifest["source"]["record_count"] == manifest["output"]["row_count"] == 5
    assert manifest["source"]["column_count"] == manifest["output"]["column_count"] == 41
    assert manifest["source"]["columns"] == list(HOUSING.expected_columns)
    assert read_latest(config.bronze_dir, KEY)["snapshot_id"] == result.snapshot_id


def test_rerun_reuses_the_building_snapshot(config) -> None:
    first = ingest_dataset(config, KEY)
    second = ingest_dataset(config, KEY)
    assert (first.status, second.status) == ("created", "reused")
    assert second.snapshot_dir == first.snapshot_dir


def test_migration_keeps_old_snapshot_but_activates_the_new_one(tmp_path: Path) -> None:
    """Mirror of the real migration: project-level snapshot first, then building-level."""
    old_config = housing_config(tmp_path, PROJECT_COLUMNS, "Affordable_Housing_*_by_Project_*.csv")
    write_csv(old_config.source_dir / PROJECT_FILE, [list(PROJECT_COLUMNS), ["1"] * 19])
    old = ingest_dataset(old_config, KEY)
    old_bytes = old.parquet_path.read_bytes()

    new_config = housing_config(tmp_path)
    shutil.copyfile(FIXTURE, new_config.source_dir / BUILDING_FILE)
    new = ingest_dataset(new_config, KEY)

    assert new.snapshot_id != old.snapshot_id
    assert old.parquet_path.read_bytes() == old_bytes  # history kept untouched
    assert read_latest(new_config.bronze_dir, KEY)["snapshot_id"] == new.snapshot_id


# --- schema changes -------------------------------------------------------------


def test_project_level_file_cannot_pass_as_building_data(config) -> None:
    write_csv(config.source_dir / BUILDING_FILE, [list(PROJECT_COLUMNS), ["1"] * 19])
    with pytest.raises(SourceError, match="missing expected columns"):
        ingest_dataset(config, KEY)
    assert read_latest(config.bronze_dir, KEY) is None


def test_unexpected_extra_column_is_kept_and_reported(config) -> None:
    rows = read_csv(FIXTURE)
    write_csv(
        config.source_dir / BUILDING_FILE,
        [[*rows[0], "New Column"], *[[*row, "x"] for row in rows[1:]]],
    )
    result = ingest_dataset(config, KEY)
    assert pl.read_parquet(result.parquet_path).columns[-1] == "New Column"
    contract = next(c for c in result.checks if c.name == "schema_matches_contract")
    assert not contract.passed and contract.details["extra"] == ["New Column"]


def test_missing_geography_column_fails(config) -> None:
    rows = read_csv(FIXTURE)
    drop = rows[0].index("Latitude")
    write_csv(config.source_dir / BUILDING_FILE, [r[:drop] + r[drop + 1 :] for r in rows])
    with pytest.raises(SourceError, match="Latitude"):
        ingest_dataset(config, KEY)
