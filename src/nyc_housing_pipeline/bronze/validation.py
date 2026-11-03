"""Checks on the Bronze output and read-only diagnostics.

Blocking checks must pass before a snapshot is published. Warning checks only
report (for example duplicate rows): Bronze never acts on them.
"""

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl

from nyc_housing_pipeline.bronze.source import Record

# name -> (description, blocking). Also used as the Dagster asset-check definitions.
CHECKS: dict[str, tuple[str, bool]] = {
    "source_unchanged": (
        "Source size and modification time were identical before and after conversion.",
        True,
    ),
    "schema_matches_source": (
        "Bronze has the CSV header's column names in the same order, all stored as String.",
        True,
    ),
    "row_count_matches_source": (
        "Bronze row count equals the record count of the independent CSV parse.",
        True,
    ),
    "sample_values_match_source": (
        "Sampled records are identical in source and Bronze, including null vs empty string.",
        True,
    ),
    "schema_matches_contract": (
        "Warning only: the source header differs from the configured columns "
        "(extra or re-ordered columns are kept as-is).",
        False,
    ),
    "no_duplicate_rows": ("Warning only: count of rows that exactly repeat another row.", False),
    "identifiers_unique": (
        "Warning only: identifier columns have no nulls, empty strings or repeated values.",
        False,
    ),
}


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: dict[str, Any]

    @property
    def blocking(self) -> bool:
        return CHECKS[self.name][1]

    @property
    def failed_blocking(self) -> bool:
        return self.blocking and not self.passed

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_contract(columns: Sequence[str], expected: Sequence[str]) -> CheckResult:
    return CheckResult(
        "schema_matches_contract",
        list(columns) == list(expected),
        {
            "missing": [c for c in expected if c not in columns],
            "extra": [c for c in columns if c not in expected],
        },
    )


def check_output(
    parquet: Path, columns: Sequence[str], record_count: int, sample: Mapping[int, Record]
) -> list[CheckResult]:
    """Compare the Parquet file with the independently parsed source."""
    frame = pl.scan_parquet(parquet, glob=False)
    schema = frame.collect_schema()
    schema_ok = schema.names() == list(columns) and all(t == pl.String for t in schema.dtypes())
    rows = frame.select(pl.len()).collect().item()
    mismatches = sample_mismatches(frame, columns, sample) if schema_ok else ["schema differs"]
    return [
        CheckResult(
            "schema_matches_source",
            schema_ok,
            {"bronze_schema": {name: str(dtype) for name, dtype in schema.items()}},
        ),
        CheckResult(
            "row_count_matches_source",
            rows == record_count,
            {"source": record_count, "bronze": rows},
        ),
        CheckResult(
            "sample_values_match_source",
            not mismatches,
            {
                "sampled_records": len(sample),
                "mismatch_count": len(mismatches),
                "first_mismatches": mismatches[:10],
            },
        ),
    ]


def sample_mismatches(
    frame: pl.LazyFrame, columns: Sequence[str], sample: Mapping[int, Record]
) -> list[str]:
    rows = frame.with_row_index("__row__").filter(pl.col("__row__").is_in(list(sample))).collect()
    bronze = {row[0]: row[1:] for row in rows.iter_rows()}
    mismatches = []
    for index, source_row in sample.items():
        if index not in bronze:
            mismatches.append(f"record {index}: missing in Bronze")
            continue
        for column, source_value, bronze_value in zip(
            columns, source_row, bronze[index], strict=True
        ):
            if source_value != bronze_value:
                mismatches.append(
                    f"record {index}, {column}: source={source_value!r:.80} "
                    f"bronze={bronze_value!r:.80}"
                )
    return mismatches


def profile_output(
    parquet: Path, identifier_columns: Sequence[str]
) -> tuple[dict[str, Any], list[CheckResult]]:
    """Count nulls, empty strings and duplicates. Reads only; changes nothing.

    Duplicate rows are counted with a 64-bit hash per row (bounded memory). For ~4M
    rows the chance of any hash collision is about 4e-7.
    """
    frame = pl.scan_parquet(parquet, glob=False)
    ids = [c for c in identifier_columns if c in frame.collect_schema().names()]
    stats = (
        frame.select(
            pl.len().alias("rows"),
            pl.struct(pl.all()).hash(seed=0).n_unique().alias("unique_rows"),
            pl.all().null_count().name.suffix(" :nulls"),
            (pl.all() == "").sum().name.suffix(" :empty"),
            *[pl.col(c).drop_nulls().n_unique().alias(f"{c} :distinct") for c in ids],
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    columns = [name[: -len(" :nulls")] for name in stats if name.endswith(" :nulls")]
    rows = stats["rows"]
    duplicate_rows = rows - stats["unique_rows"]
    identifiers = {
        c: {
            "nulls": stats[f"{c} :nulls"],
            "empty_strings": stats[f"{c} :empty"],
            "repeated_values": rows - stats[f"{c} :nulls"] - stats[f"{c} :distinct"],
        }
        for c in ids
    }
    profile = {
        "rows": rows,
        "duplicate_rows": duplicate_rows,
        "null_counts": {c: stats[f"{c} :nulls"] for c in columns},
        "empty_string_counts": {c: stats[f"{c} :empty"] for c in columns},
        "identifiers": identifiers,
    }
    checks = [
        CheckResult("no_duplicate_rows", duplicate_rows == 0, {"duplicate_rows": duplicate_rows}),
        CheckResult(
            "identifiers_unique",
            len(ids) == len(identifier_columns)
            and not any(any(counts.values()) for counts in identifiers.values()),
            {
                "identifiers": identifiers,
                "missing_columns": sorted(set(identifier_columns) - set(ids)),
            },
        ),
    ]
    return profile, checks
