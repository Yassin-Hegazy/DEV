"""CSV -> Parquet keeps the source exactly: Bronze performs no cleaning."""

from pathlib import Path

import polars as pl
import pytest

from nyc_housing_pipeline.bronze.conversion import scan_source_csv, write_parquet
from nyc_housing_pipeline.config import ParquetSettings
from tests.conftest import COLUMNS, EDGE_CASE_ROWS, FIXTURES_DIR

SETTINGS = ParquetSettings(compression="zstd", compression_level=3, row_group_size=None)


def convert(source: Path, tmp_path: Path) -> pl.DataFrame:
    out = tmp_path / "out.parquet"
    write_parquet(scan_source_csv(source), out, SETTINGS, {"snapshot_id": "abc"})
    return pl.read_parquet(out)


@pytest.fixture
def frame(tmp_path: Path) -> pl.DataFrame:
    return convert(FIXTURES_DIR / "edge_cases.csv", tmp_path)


def test_scan_is_lazy() -> None:
    assert isinstance(scan_source_csv(FIXTURES_DIR / "edge_cases.csv"), pl.LazyFrame)


def test_round_trip_is_exact(frame: pl.DataFrame) -> None:
    assert tuple(frame.columns) == COLUMNS
    assert frame.rows() == EDGE_CASE_ROWS
    assert set(frame.dtypes) == {pl.String}


def test_file_metadata_is_embedded(tmp_path: Path) -> None:
    convert(FIXTURES_DIR / "edge_cases.csv", tmp_path)
    assert pl.read_parquet_metadata(tmp_path / "out.parquet")["snapshot_id"] == "abc"


# One test per cleaning step that Bronze must NOT do.


def test_duplicate_rows_are_kept(frame: pl.DataFrame) -> None:
    assert frame.height == 9
    assert frame.row(0) == frame.row(6)


def test_all_null_row_is_kept(frame: pl.DataFrame) -> None:
    assert frame.row(8) == (None,) * 6


def test_null_and_empty_string_stay_distinct(frame: pl.DataFrame) -> None:
    assert frame["Units"][1] is None  # unquoted empty
    assert frame["Comments"][1] == ""  # quoted empty


def test_leading_zeros_are_kept(frame: pl.DataFrame) -> None:
    assert (frame["PermitNumber"][3], frame["SequenceNumber"][0], frame["Units"][2]) == (
        "0000123",
        "0001",
        "007",
    )


def test_whitespace_is_not_trimmed(frame: pl.DataFrame) -> None:
    assert (frame["BoroughName"][1], frame["Units"][5]) == ("Brooklyn ", "  3 ")


def test_borough_names_are_not_standardized(frame: pl.DataFrame) -> None:
    assert frame["BoroughName"].to_list()[:4] == ["BROOKLYN", "Brooklyn ", "", "bk"]


def test_dates_are_not_parsed(frame: pl.DataFrame) -> None:
    assert frame["IssueDate"].to_list()[2:5] == ["13/45/2026", "not a date", "2026-02-30"]


def test_quotes_newlines_and_unicode_survive(frame: pl.DataFrame) -> None:
    assert frame["Comments"].to_list()[3:6] == [
        "Comma, inside quotes",
        "Line one\nline two",
        "Unicode: café – Bronx 🏙 日本",
    ]
    assert frame["Comments"][7] == 'Embedded "quotes" here'


def test_crlf_source(tmp_path: Path) -> None:
    source = tmp_path / "crlf.csv"
    source.write_bytes(b'"a","b"\r\n"1","x\r\ny"\r\n"2",\r\n')
    assert convert(source, tmp_path).rows() == [("1", "x\r\ny"), ("2", None)]


def test_header_only_source(tmp_path: Path) -> None:
    frame = convert(FIXTURES_DIR / "header_only.csv", tmp_path)
    assert (frame.height, tuple(frame.columns)) == (0, COLUMNS)


def test_too_many_fields_raises(tmp_path: Path) -> None:
    with pytest.raises(pl.exceptions.ComputeError):
        convert(FIXTURES_DIR / "malformed_long_row.csv", tmp_path)


def test_polars_alone_pads_short_rows(tmp_path: Path) -> None:
    """Why the independent scan exists: Polars hides this malformed row."""
    frame = convert(FIXTURES_DIR / "malformed_short_row.csv", tmp_path)
    assert frame.row(1)[3:] == (None, None, None)
