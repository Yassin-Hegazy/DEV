"""Stream a source CSV into one Parquet file with the Polars Lazy API.

Bronze reads but never interprets:
* every column is ``String`` (no type inference, no date parsing), so "0001" stays "0001";
* an unquoted empty field (``a,,b``) becomes null, a quoted empty field (``a,"",b``) stays "";
* no text such as "NA" or "NULL" is treated as null, whitespace is kept;
* rows with too many fields raise an error, parse errors are never ignored;
* no row is filtered, deduplicated, re-ordered or changed.
"""

from dataclasses import asdict
from pathlib import Path

import polars as pl

from nyc_housing_pipeline.config import ParquetSettings

# Bump when the Bronze representation changes; it is part of the snapshot id.
FORMAT_VERSION = 1

CSV_READ_OPTIONS = {
    "has_header": True,
    "separator": ",",
    "quote_char": '"',
    "encoding": "utf8",
    "infer_schema": False,  # all columns String
    "try_parse_dates": False,
    "null_values": None,
    "empty_string_is_null": True,  # only *unquoted* empty fields -> null
    "truncate_ragged_lines": False,
    "ignore_errors": False,
    "glob": False,  # source filenames contain "(" and ")"
}


def scan_source_csv(path: Path) -> pl.LazyFrame:
    return pl.scan_csv(path, **CSV_READ_OPTIONS)


def write_parquet(
    frame: pl.LazyFrame, path: Path, settings: ParquetSettings, file_metadata: dict[str, str]
) -> None:
    """Stream ``frame`` into a single Parquet file, keeping the source row order."""
    frame.sink_parquet(
        path,
        compression=settings.compression,  # type: ignore[arg-type]
        compression_level=settings.compression_level,
        row_group_size=settings.row_group_size,
        maintain_order=True,
        metadata=file_metadata,
        engine="streaming",
    )


def settings_fingerprint(settings: ParquetSettings) -> dict[str, object]:
    """Everything besides the source bytes that determines the Bronze output."""
    return {"format_version": FORMAT_VERSION, "csv": CSV_READ_OPTIONS, "parquet": asdict(settings)}
