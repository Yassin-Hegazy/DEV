# Architecture and Design Decisions — Bronze Milestone

## Layers and responsibilities

```
data/source/<file>.csv ──► Dagster asset bronze/<dataset> ──► data/bronze/<dataset>/snapshots/<id>/
   (external asset source/<dataset>)                            data.parquet + manifest.json
```

| Module | Single responsibility |
| --- | --- |
| `config.py` | Turn `pipeline.toml` into dataclasses. |
| `bronze/source.py` | Find the file, check the header, strictly parse every record (count and sample). |
| `bronze/conversion.py` | The Polars read/write settings and the CSV → Parquet call. |
| `bronze/validation.py` | Compare the output with the source; count nulls and duplicates. |
| `bronze/snapshots.py` | Snapshot identity, folder layout, the `latest.json` pointer. |
| `bronze/ingestion.py` | Run the steps above in order and write the manifest. |
| `orchestration/*` | Translate between Dagster and `ingest_dataset`. No data logic. |

Dependencies point one way only: `orchestration` → `bronze` → `config`/`fileio`/`runtime`.
The Bronze package has no Dagster imports, so it can be tested and run on its own.
Each check is defined once (`validation.CHECKS`). The same entry produces both the
Python result and the Dagster asset-check spec.

## Decisions

### D1. Every Bronze column is a String
Type inference would corrupt values: `SequenceNumber "0001"` would become `1`, and
`PermitStatusID "020"` would become `20`. In the permits file, 3,985,631 of the
3,985,648 `SequenceNumber` values start with a zero. Dates would be parsed in one
guessed format. Choosing types is a Silver decision, to be made after profiling.

### D2. Null vs. empty string
Socrata writes missing values as **unquoted** empty fields and quotes every real
value. Both forms appear in the permits file (for example `PermitLocationComments`
contains about 100k quoted `""`). Bronze keeps the difference: unquoted empty →
`null`, quoted empty → `""`. No text such as `NA` or `NULL` becomes null.

### D3. A second, independent parser
Polars 2.1.0 was probed directly. With `truncate_ragged_lines=False` it still
**pads short rows with nulls without an error**. It also renames duplicate headers
(`a_duplicated_0`) and turns blank lines into all-null rows. A row count taken from
Polars therefore cannot detect Polars' own mistakes.

So every record is first parsed by Python's `csv` module in strict mode with
`QUOTE_NOTNULL`, which gives the same null/`""` semantics. That pass counts the
records, rejects any record whose field count is wrong, and keeps a seeded random
sample. The Polars output must then match both the count and the sample.

The price is speed. This pass is single-threaded Python and dominates the runtime;
see the report.

### D4. One Parquet file per snapshot
About 200 MB for 4M rows. Splitting it into many small files would add overhead
with no measured benefit. Partitioning and row-group tuning depend on how Silver
will read the data, so both are deferred to benchmarking. Both settings are part
of the snapshot fingerprint.

### D5. Content-addressed snapshots with an atomic publish
`snapshot_id = sha256(source)[:16] + "_" + sha256(settings)[:8]`.

- Re-running on unchanged input verifies the existing snapshot and reuses it.
- Output is written to `_staging_<run_id>/`, validated, then renamed into
  `snapshots/<id>/` in one operation. A reader never sees a half-written snapshot.
  If anything fails, only the staging folder is deleted.
- Incremental ingestion can be added later as additional snapshots.

### D6. Diagnostics report, never act
Duplicate rows, identifier problems and schema drift are recorded and appear as
WARN checks. Whether, for example, repeated identifiers count as duplicates is a
Silver modeling question. Duplicate rows are counted with a 64-bit hash per row,
which keeps memory bounded. The chance of a collision across 4M rows is about 4e-7.

### D7. Column contract
`expected_columns` in the config records the header observed in the supplied
files. A missing column fails ingestion, because downstream code would break.
Extra or re-ordered columns are kept as-is and reported as a warning.

## Known limitations

- **Format:** UTF-8, comma-delimited, `"`-quoted CSV only. A BOM is tolerated.
  Other encodings are refused, not guessed.
- **Change detection:** `source_unchanged` compares size and modification time. A
  write that preserves both would not be detected; the SHA-256 in the manifest is
  the authoritative record of the bytes that were ingested.
- **Disk use:** each new source version adds a full Parquet copy, and there is no
  retention policy yet.
- **Source retention:** the CSV is not copied into the snapshot (that would add
  about 3 GB). Keep the source files; the manifest checksum identifies them.
- **One volume:** the publish step is a directory rename, so `data/bronze` must live
  on a single filesystem.
- **Memory measurement:** `peak_memory_bytes` is the OS-reported peak of the whole
  process. Under Dagster each asset runs in its own process, so the figure is close
  to per-asset; in tests, assets share a process.
