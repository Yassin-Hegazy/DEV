"""Find the source CSV, read its header, and verify every record independently of Polars.

Why a second parser: Polars silently pads short rows with nulls, renames duplicate
headers and turns blank lines into all-null rows. Python's ``csv`` module (strict
mode) catches all of those, so it counts records and samples values that the
Bronze output is checked against. Nothing here writes to the source file.
"""

import csv
import random
from dataclasses import dataclass
from pathlib import Path

from nyc_housing_pipeline.errors import SourceError

# Matches the Polars settings in conversion.py. QUOTE_NOTNULL returns None for an
# unquoted empty field and "" for a quoted empty field - the same null/"" split Bronze keeps.
CSV_DIALECT = {"delimiter": ",", "quotechar": '"', "strict": True, "quoting": csv.QUOTE_NOTNULL}

# The stdlib default (128 KiB per field) is too small for long WKT geometries.
csv.field_size_limit(2**31 - 1)

Record = tuple[str | None, ...]


@dataclass(frozen=True)
class RecordScan:
    record_count: int
    physical_lines: int  # including the header and embedded newlines
    sample: dict[int, Record]  # 0-based record index -> values


def find_source(source_dir: Path, pattern: str) -> Path:
    """Return the one file in ``source_dir`` matching ``pattern``."""
    matches = sorted(p for p in source_dir.glob(pattern) if p.is_file())
    if len(matches) != 1:
        raise SourceError(
            f"Expected exactly one file matching '{pattern}' in {source_dir}, "
            f"found {len(matches)}: {[p.name for p in matches]}"
        )
    return matches[0]


def read_header(path: Path) -> tuple[str, ...]:
    """Read the header and refuse names that cannot be kept exactly as-is."""
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle, **CSV_DIALECT), None)
    except (OSError, csv.Error, UnicodeDecodeError) as exc:
        raise SourceError(f"Cannot read the header of {path.name}: {exc}") from exc
    if not header:
        raise SourceError(f"{path.name} is empty: no header row")
    if None in header or "" in header:
        raise SourceError(f"{path.name} has an empty column name in its header")
    duplicates = sorted({name for name in header if header.count(name) > 1})
    if duplicates:
        raise SourceError(f"{path.name} has duplicate column names: {duplicates}")
    return tuple(header)


def scan_records(path: Path, column_count: int, sample_size: int, seed: int) -> RecordScan:
    """Strictly parse every record: count them, check field counts, keep a sample.

    The sample is a seeded reservoir sample (uniform over the file) plus the last record.
    """
    rng = random.Random(seed)
    reservoir: list[tuple[int, Record]] = []
    count = 0
    values: Record = ()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, **CSV_DIALECT)
        try:
            next(reader)  # header (already checked by read_header)
            for row in reader:
                if len(row) != column_count:
                    raise SourceError(
                        f"{path.name}: record {count + 1} (line {reader.line_num}) has "
                        f"{len(row)} fields, expected {column_count}. Bronze never pads, "
                        "truncates or skips records."
                    )
                values = tuple(row)
                if count < sample_size:
                    reservoir.append((count, values))
                elif (slot := rng.randrange(count + 1)) < sample_size:
                    reservoir[slot] = (count, values)
                count += 1
        except (csv.Error, UnicodeDecodeError) as exc:
            raise SourceError(
                f"{path.name}: malformed CSV at record {count + 1} (line {reader.line_num}): {exc}"
            ) from exc
    sample = dict(reservoir)
    if count:
        sample[count - 1] = values  # always compare the last record too
    return RecordScan(count, reader.line_num, dict(sorted(sample.items())))
