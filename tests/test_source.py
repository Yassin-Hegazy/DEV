"""Source discovery, header checks and the independent record scan."""

from pathlib import Path

import pytest

from nyc_housing_pipeline.bronze.source import find_source, read_header, scan_records
from nyc_housing_pipeline.errors import SourceError
from tests.conftest import COLUMNS, EDGE_CASE_ROWS, FIXTURES_DIR

EDGE = FIXTURES_DIR / "edge_cases.csv"


def write(tmp_path: Path, data: bytes) -> Path:
    path = tmp_path / "source.csv"
    path.write_bytes(data)
    return path


# --- find_source -------------------------------------------------------------


def test_find_source_returns_the_single_match(tmp_path: Path) -> None:
    (tmp_path / "Street_Construction_Permits_(2022_-_Present)_20261103.csv").write_text("a\n")
    (tmp_path / "notes.txt").write_text("x")
    found = find_source(tmp_path, "Street_Construction_Permits_(2022_-_Present)_*.csv")
    assert found.name == "Street_Construction_Permits_(2022_-_Present)_20261103.csv"


@pytest.mark.parametrize("files", [[], ["Data_1.csv", "Data_2.csv"]], ids=["none", "two"])
def test_find_source_refuses_zero_or_many_matches(tmp_path: Path, files: list[str]) -> None:
    for name in files:
        (tmp_path / name).write_text("a\n")
    with pytest.raises(SourceError, match=f"found {len(files)}"):
        find_source(tmp_path, "Data_*.csv")


def test_find_source_in_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="found 0"):
        find_source(tmp_path / "missing", "*.csv")


# --- read_header ---------------------------------------------------------------


def test_header_is_read_in_order() -> None:
    assert read_header(EDGE) == COLUMNS


def test_utf8_bom_is_not_part_of_the_first_name(tmp_path: Path) -> None:
    assert read_header(write(tmp_path, b'\xef\xbb\xbf"a","b"\n')) == ("a", "b")


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (b"", "empty"),
        (b'"a",,"c"\n', "empty column name"),
        (b'"a","a"\n', "duplicate column names"),
        (b'"caf\xe9"\n', "Cannot read"),
    ],
    ids=["zero-bytes", "empty-name", "duplicate-name", "invalid-utf8"],
)
def test_bad_headers_are_refused(tmp_path: Path, data: bytes, message: str) -> None:
    with pytest.raises(SourceError, match=message):
        read_header(write(tmp_path, data))


# --- scan_records ----------------------------------------------------------------


def test_scan_counts_records_not_lines() -> None:
    scan = scan_records(EDGE, len(COLUMNS), sample_size=1000, seed=1)
    assert scan.record_count == 9
    assert scan.physical_lines == 11  # header + 9 records + 1 embedded newline


def test_scan_keeps_null_vs_empty_string() -> None:
    scan = scan_records(EDGE, len(COLUMNS), sample_size=1000, seed=1)
    assert list(scan.sample.values()) == EDGE_CASE_ROWS


def test_sample_is_reproducible_and_includes_last_record(tmp_path: Path) -> None:
    rows = "".join(f'"{i:05d}",{i}\n' for i in range(500))
    path = write(tmp_path, f'"id","n"\n{rows}'.encode())
    first = scan_records(path, 2, sample_size=20, seed=42)
    assert first.sample == scan_records(path, 2, sample_size=20, seed=42).sample
    assert len(first.sample) <= 21
    assert first.sample[499] == ("00499", "499")


def test_scan_handles_crlf(tmp_path: Path) -> None:
    path = write(tmp_path, b'"a","b"\r\n"1","x\r\ny"\r\n"2",\r\n')
    assert scan_records(path, 2, 10, 0).sample == {0: ("1", "x\r\ny"), 1: ("2", None)}


def test_scan_of_header_only_file() -> None:
    scan = scan_records(FIXTURES_DIR / "header_only.csv", len(COLUMNS), 10, 0)
    assert (scan.record_count, scan.sample) == (0, {})


@pytest.mark.parametrize(
    ("fixture", "message"),
    [
        ("malformed_short_row.csv", "record 2 .*3 fields, expected 6"),
        ("malformed_long_row.csv", "record 2 .*7 fields, expected 6"),
        ("malformed_blank_line.csv", "record 2 .*0 fields, expected 6"),
        ("malformed_unterminated_quote.csv", "malformed CSV at record 2"),
    ],
)
def test_malformed_records_are_refused(fixture: str, message: str) -> None:
    with pytest.raises(SourceError, match=message):
        scan_records(FIXTURES_DIR / fixture, len(COLUMNS), 10, 0)


def test_invalid_utf8_in_body_is_refused(tmp_path: Path) -> None:
    path = write(tmp_path, b'"a","b"\n"ok","1"\n"\xff\xfe","2"\n')
    with pytest.raises(SourceError, match="malformed CSV"):
        scan_records(path, 2, 10, 0)


def test_fields_longer_than_csv_default_limit(tmp_path: Path) -> None:
    big = "x" * 300_000  # e.g. a long WKT geometry; stdlib default limit is 128 KiB
    path = write(tmp_path, f'"id","wkt"\n"1","{big}"\n'.encode())
    assert scan_records(path, 2, 10, 0).sample[0] == ("1", big)
