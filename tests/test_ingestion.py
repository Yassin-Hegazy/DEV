"""End-to-end ingestion: snapshots, manifests, idempotency and failure safety."""

import json
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from nyc_housing_pipeline.bronze import conversion, ingestion
from nyc_housing_pipeline.bronze.ingestion import RUN_LOG_FILE, ingest_dataset
from nyc_housing_pipeline.bronze.snapshots import read_latest
from nyc_housing_pipeline.bronze.validation import CHECKS
from nyc_housing_pipeline.errors import SourceError, ValidationError
from tests.conftest import COLUMNS, DATASET_KEY, EDGE_CASE_ROWS, SOURCE_FILENAME, Sandbox, sha256_of

MakeProject = Callable[..., Sandbox]


def ingest(project: Sandbox, **kwargs) -> ingestion.IngestionResult:
    return ingest_dataset(project.config, DATASET_KEY, **kwargs)


def dataset_dir(project: Sandbox) -> Path:
    return project.config.bronze_dir / DATASET_KEY


def snapshot_dirs(project: Sandbox) -> list[Path]:
    root = dataset_dir(project) / "snapshots"
    return sorted(root.iterdir()) if root.exists() else []


def no_staging_left(project: Sandbox) -> bool:
    return not list(dataset_dir(project).glob("_staging_*"))


def run_log(project: Sandbox) -> list[dict]:
    lines = (project.config.log_dir / RUN_LOG_FILE).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def latest_id(project: Sandbox) -> str | None:
    pointer = read_latest(project.config.bronze_dir, DATASET_KEY)
    return pointer and pointer["snapshot_id"]


# --- a successful ingestion --------------------------------------------------------


def test_publishes_a_faithful_snapshot(make_project: MakeProject) -> None:
    project = make_project()
    result = ingest(project)
    assert result.status == "created"
    assert pl.read_parquet(result.parquet_path).rows() == EDGE_CASE_ROWS
    assert {c.name for c in result.checks} == set(CHECKS)
    assert not any(c.failed_blocking for c in result.checks)
    assert latest_id(project) == result.snapshot_id
    assert no_staging_left(project)


def test_manifest_traces_the_source(make_project: MakeProject) -> None:
    project = make_project()
    result = ingest(project, orchestrator_run_id="run-123")
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    source_file = project.config.source_dir / SOURCE_FILENAME

    assert manifest == result.manifest
    assert manifest["source"] | {"columns": None} == manifest["source"] | {
        "filename": SOURCE_FILENAME,
        "path": f"data/source/{SOURCE_FILENAME}",
        "size_bytes": source_file.stat().st_size,
        "sha256": sha256_of(source_file),
        "record_count": 9,
        "column_count": 6,
        "columns": None,
    }
    assert manifest["source"]["columns"] == list(COLUMNS)
    assert manifest["output"]["sha256"] == sha256_of(result.parquet_path)
    assert manifest["output"]["row_count"] == 9
    assert manifest["ingestion"]["orchestrator_run_id"] == "run-123"
    assert manifest["ingestion"]["started_at_utc"].endswith("Z")
    assert manifest["environment"]["packages"]["polars"] == pl.__version__
    assert manifest["snapshot_id"].startswith(manifest["source"]["sha256"][:16])
    assert pl.read_parquet_metadata(result.parquet_path)["source_sha256"] == sha256_of(source_file)


def test_source_file_is_never_modified(make_project: MakeProject) -> None:
    project = make_project()
    source_file = project.config.source_dir / SOURCE_FILENAME
    before = (source_file.read_bytes(), source_file.stat().st_mtime_ns)
    ingest(project)
    ingest(project)
    assert (source_file.read_bytes(), source_file.stat().st_mtime_ns) == before


def test_duplicates_are_reported_not_removed(make_project: MakeProject) -> None:
    result = ingest(make_project())
    duplicates = next(c for c in result.checks if c.name == "no_duplicate_rows")
    assert not duplicates.passed and not duplicates.blocking
    assert pl.read_parquet(result.parquet_path).height == 9


def test_empty_dataset(make_project: MakeProject) -> None:
    result = ingest(make_project("header_only.csv"))
    frame = pl.read_parquet(result.parquet_path)
    assert (frame.height, tuple(frame.columns)) == (0, COLUMNS)


def test_extra_column_is_kept_and_reported(make_project: MakeProject) -> None:
    result = ingest(make_project("extra_column.csv"))
    assert pl.read_parquet(result.parquet_path)["UnexpectedColumn"].to_list() == ["surprise", None]
    contract = next(c for c in result.checks if c.name == "schema_matches_contract")
    assert not contract.passed and contract.details["extra"] == ["UnexpectedColumn"]


def test_run_log_records_every_attempt(make_project: MakeProject) -> None:
    project = make_project()
    ingest(project)
    ingest(project)
    project.place_source("malformed_short_row.csv")
    with pytest.raises(SourceError):
        ingest(project)
    assert [e["status"] for e in run_log(project)] == ["created", "reused", "failed"]
    assert run_log(project)[-1]["error_type"] == "SourceError"


# --- idempotency and snapshots -------------------------------------------------


def test_rerun_reuses_the_snapshot(make_project: MakeProject) -> None:
    project = make_project()
    first = ingest(project)
    files_before = {p: p.stat().st_mtime_ns for p in first.snapshot_dir.iterdir()}
    second = ingest(project)
    assert second.status == "reused"
    assert snapshot_dirs(project) == [first.snapshot_dir]
    assert {p: p.stat().st_mtime_ns for p in first.snapshot_dir.iterdir()} == files_before
    assert {c.name for c in second.checks} == set(CHECKS)


def test_new_source_makes_a_new_snapshot_and_keeps_the_old(make_project: MakeProject) -> None:
    project = make_project()
    first = ingest(project)
    old_bytes = first.parquet_path.read_bytes()
    project.place_source("extra_column.csv")
    second = ingest(project)
    assert second.snapshot_id != first.snapshot_id
    assert len(snapshot_dirs(project)) == 2
    assert first.parquet_path.read_bytes() == old_bytes
    assert latest_id(project) == second.snapshot_id


def test_reverted_source_reuses_and_repoints_latest(make_project: MakeProject) -> None:
    project = make_project()
    first = ingest(project)
    project.place_source("extra_column.csv")
    ingest(project)
    project.place_source("edge_cases.csv")
    assert ingest(project).status == "reused"
    assert latest_id(project) == first.snapshot_id


def test_corrupted_snapshot_is_reported_not_overwritten(make_project: MakeProject) -> None:
    project = make_project()
    first = ingest(project)
    first.parquet_path.write_bytes(b"tampered")
    with pytest.raises(ValidationError, match="corrupted"):
        ingest(project)
    assert first.parquet_path.read_bytes() == b"tampered"


# --- failures never touch published data --------------------------------------


@pytest.mark.parametrize(
    "bad_source",
    [
        "malformed_short_row.csv",
        "malformed_long_row.csv",
        "malformed_unterminated_quote.csv",
        "malformed_blank_line.csv",
        "duplicate_header.csv",
        "missing_column.csv",
        b"",
        b'"PermitNumber","SequenceNumber","BoroughName","IssueDate","Comments","Units"\n'
        b'"\xff","1","2","3","4","5"\n',
    ],
    ids=["short", "long", "quote", "blank", "dup-header", "missing-col", "empty", "bad-utf8"],
)
def test_bad_source_fails_without_touching_published_data(
    make_project: MakeProject, bad_source: str | bytes
) -> None:
    project = make_project()
    good = ingest(project)
    good_bytes = good.parquet_path.read_bytes()

    project.place_source(bad_source)
    with pytest.raises(SourceError):
        ingest(project)

    assert snapshot_dirs(project) == [good.snapshot_dir]
    assert good.parquet_path.read_bytes() == good_bytes
    assert latest_id(project) == good.snapshot_id
    assert no_staging_left(project)


def test_missing_source_file(make_project: MakeProject) -> None:
    with pytest.raises(SourceError, match="found 0"):
        ingest(make_project(None))


@pytest.mark.parametrize(
    ("tamper", "failed_check"),
    [
        (lambda lf: lf.head(8), "row_count_matches_source"),
        (lambda lf: lf.unique(maintain_order=True), "row_count_matches_source"),
        (
            lambda lf: lf.with_columns(pl.col("BoroughName").str.strip_chars()),
            "sample_values_match_source",
        ),
        (
            lambda lf: lf.with_columns(pl.col("Units").cast(pl.Int64, strict=False)),
            "schema_matches_source",
        ),
    ],
    ids=["drop-row", "deduplicate", "trim-whitespace", "cast-number"],
)
def test_cleaning_during_conversion_blocks_publication(
    make_project: MakeProject, monkeypatch: pytest.MonkeyPatch, tamper, failed_check: str
) -> None:
    project = make_project()
    good = ingest(project)
    monkeypatch.setattr(conversion, "FORMAT_VERSION", 999)  # force a fresh conversion
    original = conversion.scan_source_csv
    monkeypatch.setattr(ingestion, "scan_source_csv", lambda path: tamper(original(path)))

    with pytest.raises(ValidationError, match=failed_check):
        ingest(project)

    assert snapshot_dirs(project) == [good.snapshot_dir]
    assert latest_id(project) == good.snapshot_id
    assert no_staging_left(project)
