# Bronze Milestone Report

**Date:** 2026-11-03 · **Scope:** source → Bronze ingestion · **Status:** complete

## 1. Summary

Both source datasets are now stored as validated, all-string Parquet snapshots,
each traceable to the exact source bytes. The street permits CSV is 2.9 GB and
3,985,648 records. It becomes a 200 MB Parquet file (14.6× smaller) whose content
was checked against an independent parse of the CSV. The conversion itself takes
about 2–7 s. Most of the ~2.5-minute ingestion is spent on that independent check
(section 6).

## 2. What was built

| Part | Result |
| --- | --- |
| Repository | uv project with a locked environment, Ruff, pytest, `.gitignore` that keeps data out of git |
| Ingestion | Find source → check header → hash → independent strict parse → Polars Lazy streaming CSV→Parquet → validate → atomic publish |
| Snapshots | Content-addressed (`<source sha>_<settings hash>`), idempotent, with `manifest.json`, `latest.json` and a run log |
| Checks | 4 blocking (source unchanged, schema, row count, sampled values) and 3 warnings (column contract, duplicates, identifiers) |
| Orchestration | Two independent Dagster assets with lineage to their source CSVs, asset checks, and one job for both |
| Tests | 78 automated tests on synthetic fixtures (all pass) |

## 3. Data ingested

| | Housing production (by Project) | Street construction permits |
| --- | --- | --- |
| NYC Open Data ID | `hq68-rnsi` | `tqtj-sjs8` |
| Source size | 984,336 B | 2,921,784,172 B |
| Records × columns | 5,928 × 19 | 3,985,648 × 39 |
| Physical lines | 5,929 | 3,994,439 (values contain embedded newlines) |
| Bronze Parquet | 141,509 B | 200,483,034 B |
| Compression ratio | 6.96× | 14.57× |
| Snapshot id | `4207820f388370b5_903f4c1a` | `7b46f29d1803a24f_903f4c1a` |

The housing file is the **by Project** dataset. The brief described **by Building**
(`hg8x-zxpr`, about 9,551 rows and 41 columns). This should be confirmed before
Silver design starts.

## 4. Validation results

All 7 checks passed for both datasets. The permits figures:

- **Row count:** 3,985,648 in Bronze = 3,985,648 counted by the independent parser.
- **Sampled values:** 1,001 records × 39 columns compared, 0 mismatches (null vs `""` included).
- **Schema:** 39 columns, header order, all `String`.
- **Diagnostics:** 0 exact duplicate rows, and `PermitNumber` is unique with no nulls.
  17 rows have a null `ApplicationTrackingID` and a null `SequenceNumber`. They were
  kept unchanged and are left for Silver to decide on.
- **Why strings:** 3,985,631 `SequenceNumber` values start with `0` (e.g. `0001`).
  Numeric type inference would have changed them.

Idempotency was confirmed on the real data: re-running both assets reused the
existing snapshots (the source was re-hashed, nothing was rewritten).

## 5. Where the time goes (permits, one Dagster run)

| Step | Seconds | Share |
| --- | ---: | ---: |
| Hash source (SHA-256) | 7.5 | 5% |
| **Independent strict parse (Python `csv`)** | **125.7** | **87%** |
| CSV → Parquet (Polars streaming) | 6.5 | 5% |
| Validate output | 0.7 | <1% |
| Diagnostics (nulls, duplicates) | 3.1 | 2% |
| **Total** | **144.0** | |

Peak memory of the Dagster step process was 6.31 GB (OS-reported peak for the
whole process).

## 6. How Arrow and Polars make the conversion fast

Polars keeps data in memory in the **Apache Arrow columnar format**: each column is
a few contiguous buffers rather than a collection of separate objects. Its CSV
reader is written in Rust, splits the file into chunks, parses the chunks on all
cores, and writes the fields straight into Arrow column buffers. Those buffers are
then encoded into Parquet column chunks without rebuilding any rows. Each claim
below comes with a measurement.

### 6.1 Native parsing into Arrow buffers instead of Python objects

| Single thread, same 2.9 GB file | Seconds |
| --- | ---: |
| Python `csv` (creates one `str` object per field, ≈155M objects) | 112.6 – 125.7 |
| Polars, `POLARS_MAX_THREADS=1` (writes into Arrow buffers) | 15.0 |

Even on **one core**, Polars is about 8× faster. Most of the gap is the cost of
creating, and later freeing, a Python object for every value.

### 6.2 Parallel parsing

| Threads | 1 | 2 | 4 | 8 | 28 |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV → Parquet (s) | 15.0 | 7.6 | 4.2 | 2.4 | 1.8 |
| Peak memory (GB) | 0.58 | 0.73 | 1.24 | 1.74 | 3.9 |

Speed-up is close to linear up to 8 threads and flattens after that. At that point
the run is bound by reading the file and writing Parquet, not by parsing. A first
1-thread attempt took 51.6 s; the repeat took 15.0 s. The first run most likely
read the file cold from disk, so it was excluded.

### 6.3 Streaming keeps memory bounded

| Strategy (28 threads) | Seconds | Peak memory |
| --- | ---: | ---: |
| Eager `read_csv` → `write_parquet` | 1.9 | **6.74 GB** |
| Lazy `scan_csv` → `sink_parquet` (what the pipeline uses) | 1.8 | **3.9 GB** |
| Lazy, 4 threads | 4.2 | 1.24 GB |

The whole permits table occupies 2.54 GB as Arrow buffers. The eager approach holds
all of it in memory, plus working space. The streaming engine processes the file
in batches and writes each batch to Parquet as soon as it is ready, so memory
depends mainly on the **number of threads, not the size of the data**. If memory
becomes tight, limiting threads trades speed for memory along the table in 6.2.

### 6.4 Columnar encoding explains the 14.6× ratio

Measured from the Parquet metadata, inspected with pyarrow:

```
CSV text                           2,922 MB
after dictionary/RLE encoding        716 MB   (~4.1x)  every one of the 39 column chunks has a dictionary page
after Zstandard level 3              200 MB   (~3.6x)  51 row groups of ~83k rows
```

Columns that repeat a small set of values (borough, status, permit type) collapse
into small dictionaries plus short integer codes. The text that remains, such as
geometries, dates and comments, is compressed by Zstandard. The largest columns
after compression are `WKT` (19.4 MB) and `LocationGeometry` (18.9 MB).

### 6.5 What this buys the Silver and Gold layers

| Query on permits | From CSV | From Bronze Parquet |
| --- | ---: | ---: |
| Distinct `BoroughName` values | 0.82 s, 2.41 GB | **0.02 s, 0.13 GB** |
| Row count | full scan | **0.01 s** (read from the file footer) |

Parquet lets a query read only the columns it needs, and the row count comes from
metadata. This is why Bronze validation of the 4M-row file takes 0.7 s.

## 7. Observations and next steps

1. **The independent check costs 87% of the runtime.** It is the price of
   catching malformed rows that Polars would silently pad. It runs only when the
   source changes; reuse skips it. Options to benchmark: run it in parallel with the
   conversion, or use a faster parser that is still independent of Polars. The
   guarantee should not be dropped.
2. **Conversion time varies between runs** (1.8 s in isolation, 2.7–6.5 s inside
   Dagster runs). Disk cache state and other processes running at the same time
   affect it. Proper benchmarks need repeated runs and a controlled cache.
3. **Memory:** the whole-process peak (6.3 GB) comes mostly from the
   diagnostics step, which hashes full rows, and from 28-thread streaming. It fits
   easily in 24 GB. `POLARS_MAX_THREADS` is the control if needed.
4. **Next milestone:** profile the Bronze data in depth, then design the Silver
   cleaning and typing.

## 8. Method and caveats

- Machine: Windows 11, 28 logical CPUs, 24 GB RAM. Python 3.13.7, Polars 2.1.0,
  Dagster 1.13.28.
- Section 5 comes from the snapshot manifest. Section 6 comes from separate
  processes (one measurement per process, for a clean peak-memory figure), run
  one after another on the same file. That file had already been read recently, so
  these are **warm OS-cache** numbers, not cold-disk numbers.
- Each figure is a single observation (except the repeated 1-thread run). These
  are indicative measurements, not a statistically rigorous benchmark.
- Peak memory is the OS-reported peak working set of the process.
