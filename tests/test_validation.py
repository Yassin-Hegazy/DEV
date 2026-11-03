"""Output checks catch lost, re-ordered, re-typed or cleaned data; diagnostics only report."""

from pathlib import Path

import polars as pl
import pytest

from nyc_housing_pipeline.bronze.validation import check_contract, check_output, profile_output
from tests.conftest import COLUMNS, EDGE_CASE_ROWS

SAMPLE = dict(enumerate(EDGE_CASE_ROWS))


def edge_frame() -> pl.DataFrame:
    return pl.DataFrame(EDGE_CASE_ROWS, schema=dict.fromkeys(COLUMNS, pl.String), orient="row")


def results(tmp_path: Path, frame: pl.DataFrame, record_count: int = 9) -> dict[str, bool]:
    path = tmp_path / "bronze.parquet"
    frame.write_parquet(path)
    return {c.name: c.passed for c in check_output(path, COLUMNS, record_count, SAMPLE)}


def test_faithful_output_passes(tmp_path: Path) -> None:
    assert results(tmp_path, edge_frame()) == {
        "schema_matches_source": True,
        "row_count_matches_source": True,
        "sample_values_match_source": True,
    }


def test_lost_row_is_detected(tmp_path: Path) -> None:
    checks = results(tmp_path, edge_frame().head(8))
    assert not checks["row_count_matches_source"]
    assert not checks["sample_values_match_source"]


def test_reordered_columns_are_detected(tmp_path: Path) -> None:
    checks = results(tmp_path, edge_frame().select(reversed(COLUMNS)))
    assert not checks["schema_matches_source"]


def test_inferred_types_are_detected(tmp_path: Path) -> None:
    frame = edge_frame().with_columns(pl.col("Units").cast(pl.Int64, strict=False))
    assert not results(tmp_path, frame)["schema_matches_source"]


@pytest.mark.parametrize(
    ("column", "row", "cleaned"),
    [("BoroughName", 1, "Brooklyn"), ("Comments", 1, None), ("PermitNumber", 3, "123")],
    ids=["trimmed", "empty-to-null", "leading-zeros-stripped"],
)
def test_cleaned_values_are_detected(tmp_path: Path, column: str, row: int, cleaned) -> None:
    frame = edge_frame().with_columns(
        pl.when(pl.int_range(pl.len()) == row)
        .then(pl.lit(cleaned, pl.String))
        .otherwise(pl.col(column))
        .alias(column)
    )
    assert not results(tmp_path, frame)["sample_values_match_source"]


def test_contract_check() -> None:
    assert check_contract(["a", "b"], ["a", "b"]).passed
    extra = check_contract(["a", "b", "c"], ["a", "b"])
    assert not extra.passed and not extra.blocking and extra.details["extra"] == ["c"]
    assert not check_contract(["b", "a"], ["a", "b"]).passed
    assert check_contract(["a"], ["a", "b"]).details["missing"] == ["b"]


def test_profile_reports_without_changing_data(tmp_path: Path) -> None:
    path = tmp_path / "bronze.parquet"
    edge_frame().write_parquet(path)
    before = path.read_bytes()
    profile, checks = profile_output(path, ["PermitNumber"])
    assert path.read_bytes() == before
    assert profile["rows"] == 9
    assert profile["duplicate_rows"] == 1
    assert profile["null_counts"]["Units"] == 2
    assert profile["empty_string_counts"]["Comments"] == 1
    assert profile["identifiers"]["PermitNumber"] == {
        "nulls": 1,
        "empty_strings": 0,
        "repeated_values": 1,
    }
    assert {c.name: c.passed for c in checks} == {
        "no_duplicate_rows": False,
        "identifiers_unique": False,
    }
    assert not any(c.blocking for c in checks)


def test_null_and_empty_rows_are_not_duplicates(tmp_path: Path) -> None:
    path = tmp_path / "b.parquet"
    pl.DataFrame({"a": ["x", "x"], "b": [None, ""]}).write_parquet(path)
    assert profile_output(path, [])[0]["duplicate_rows"] == 0


def test_profile_of_empty_dataset(tmp_path: Path) -> None:
    path = tmp_path / "b.parquet"
    pl.DataFrame(schema=dict.fromkeys(COLUMNS, pl.String)).write_parquet(path)
    profile, checks = profile_output(path, ["PermitNumber"])
    assert (profile["rows"], profile["duplicate_rows"]) == (0, 0)
    assert all(c.passed for c in checks)
