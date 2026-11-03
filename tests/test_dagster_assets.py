"""Dagster wiring: definitions, independent and combined runs, checks, failures."""

import shutil
from pathlib import Path

import dagster as dg
import pytest

from nyc_housing_pipeline.bronze.validation import CHECKS
from nyc_housing_pipeline.orchestration.bronze_assets import BRONZE_ASSETS, SOURCE_SPECS
from nyc_housing_pipeline.orchestration.definitions import defs
from nyc_housing_pipeline.orchestration.resources import PipelineConfigResource
from tests.conftest import FIXTURES_DIR, write_config

HOUSING = dg.AssetKey(["bronze", "housing_production"])
PERMITS = dg.AssetKey(["bronze", "street_construction_permits"])


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    path = write_config(
        tmp_path,
        {"housing_production": "Housing_*.csv", "street_construction_permits": "Permits_*.csv"},
    )
    source_dir = tmp_path / "data" / "source"
    shutil.copyfile(FIXTURES_DIR / "edge_cases.csv", source_dir / "Housing_1.csv")
    shutil.copyfile(FIXTURES_DIR / "extra_column.csv", source_dir / "Permits_1.csv")
    return path


def run(config_path: Path, *keys: dg.AssetKey) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [*SOURCE_SPECS, *BRONZE_ASSETS],
        selection=list(keys),
        resources={"pipeline": PipelineConfigResource(config_path=str(config_path))},
        raise_on_error=False,
    )


def materialized(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, dict]:
    return {
        e.asset_key: e.materialization.metadata for e in result.get_asset_materialization_events()
    }


def test_definitions_load() -> None:
    repo = defs.get_repository_def()
    keys = {node.key for node in repo.asset_graph.asset_nodes}
    assert {HOUSING, PERMITS, dg.AssetKey(["source", "housing_production"])} <= keys
    assert "bronze_ingestion_all" in {job.name for job in repo.get_all_jobs()}


def test_assets_declare_every_check_and_depend_on_their_source() -> None:
    for asset in BRONZE_ASSETS:
        specs = {spec.name: spec.blocking for spec in asset.check_specs}
        assert specs == {name: blocking for name, (_, blocking) in CHECKS.items()}
        source = dg.AssetKey(["source", asset.key.path[-1]])
        assert source in asset.asset_deps[asset.key]


@pytest.mark.parametrize("key", [HOUSING, PERMITS])
def test_each_asset_runs_on_its_own(config_path: Path, key: dg.AssetKey) -> None:
    result = run(config_path, key)
    assert result.success
    assert set(materialized(result)) == {key}


def test_both_assets_run_together_and_report_checks(config_path: Path) -> None:
    result = run(config_path, HOUSING, PERMITS)
    assert result.success
    assert materialized(result)[HOUSING]["dagster/row_count"].value == 9
    evaluations = {(e.asset_key, e.check_name): e for e in result.get_asset_check_evaluations()}
    assert len(evaluations) == 2 * len(CHECKS)
    assert evaluations[(HOUSING, "row_count_matches_source")].passed
    duplicates = evaluations[(HOUSING, "no_duplicate_rows")]
    assert not duplicates.passed and duplicates.severity == dg.AssetCheckSeverity.WARN


def test_rerun_reports_reuse(config_path: Path) -> None:
    first, second = run(config_path, HOUSING), run(config_path, HOUSING)
    assert materialized(first)[HOUSING]["status"].value == "created"
    assert materialized(second)[HOUSING]["status"].value == "reused"


def test_one_failure_does_not_stop_the_other_asset(config_path: Path) -> None:
    source_dir = config_path.parent.parent / "data" / "source"
    shutil.copyfile(FIXTURES_DIR / "malformed_short_row.csv", source_dir / "Housing_1.csv")
    result = run(config_path, HOUSING, PERMITS)
    assert not result.success
    assert set(materialized(result)) == {PERMITS}
    (failure,) = [e for e in result.all_events if e.is_step_failure]
    assert "SourceError" in failure.step_failure_data.user_failure_data.description
