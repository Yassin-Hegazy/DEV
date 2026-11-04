# NYC Housing & Infrastructure Data Pipeline

A batch data engineering pipeline for studying how **affordable housing
development** relates to **street construction activity** in New York City,
using public NYC Open Data.

```
Source CSV ──► Bronze (Parquet) ──► Silver ──► Gold ──► Analytics / Website
               ▲ implemented        └────── not implemented yet ──────┘
```

**Current milestone: source → Bronze ingestion only.** The pipeline turns each
manually downloaded CSV into a faithful, all-string, Zstandard-compressed Parquet
snapshot. Every snapshot is validated, described by a manifest, and traceable to
the exact source bytes it came from. Dagster orchestrates the work, and
data-quality checks gate each publication.

Results and an explanation of the performance: [docs/bronze_milestone_report.md](docs/bronze_milestone_report.md).
Design decisions: [docs/architecture.md](docs/architecture.md).

---

## Datasets

| Key | Dataset | NYC Open Data ID | Expected file in `data/source/` |
| --- | --- | --- | --- |
| `housing_production` | [Affordable Housing Production **by Building**](https://data.cityofnewyork.us/d/hg8x-zxpr) (HPD) | `hg8x-zxpr` | `Affordable_Housing_Production_by_Building_*.csv` |
| `street_construction_permits` | Street Construction Permits (2022 – Present) (DOT) | `tqtj-sjs8` | `Street_Construction_Permits_(2022_-_Present)_*.csv` |

> **Housing source migration (2026-11-03).** The first Bronze run ingested
> *Affordable Housing Production by Project* (`hq68-rnsi`), which has one row per
> project and no location fields. That was the wrong dataset. The active source is
> now *by Building* (`hg8x-zxpr`): one row per building within a project, with
> borough, ZIP code, address, BBL, BIN and latitude/longitude. Those fields are what
> the future join with street-construction permits needs.
>
> Because each row is now a building, `Project ID` repeats once for every building
> in a project, and the same building can appear in more than one project. No
> column is treated as a unique key in Bronze.
>
> The old by-Project CSV and its Bronze snapshot are no longer needed and have
> been deleted. See [docs/architecture.md](docs/architecture.md), D8.

Row counts, sizes and checksums are **measured at ingestion** and stored in each
snapshot's manifest. None of them are hard-coded.

---

## Technology choices

| Concern | Choice | Why |
| --- | --- | --- |
| Language | Python ≥ 3.13 | `csv.QUOTE_NOTNULL` (3.12+) is needed for the independent source check. |
| Processing | **Polars Lazy API** (`scan_csv` → `sink_parquet`, streaming engine) | Multi-threaded, Arrow-native, out-of-core: the 2.9 GB CSV is never loaded eagerly. |
| Storage | **Apache Parquet**, Zstandard level 3 | Columnar, compressed, readable by every analytics tool. |
| Orchestration | **Dagster** assets + asset checks | Lineage, run history, metadata and check results in a UI. |
| Tests / lint | **pytest**, **Ruff** | |
| Environment | **uv** + committed `uv.lock` | Reproducible, locked dependencies. |

Deliberately not used: Spark, Kafka, Airflow, Databricks, cloud services. This is
a single-machine batch workload. DuckDB and dbt are optional later additions, to be
introduced only when there is a concrete need.

---

## Repository structure

```
config/pipeline.toml           paths, Parquet settings, sample size, dataset column contracts
data/source/                   ← put the downloaded CSVs here (read-only to the pipeline)
data/bronze/<dataset>/
    snapshots/<snapshot_id>/data.parquet
    snapshots/<snapshot_id>/manifest.json
    latest.json                pointer to the latest ingested snapshot
logs/bronze_ingestion_runs.jsonl   one line per ingestion attempt
.dagster_home/                 local Dagster run history (only dagster.yaml is tracked)
docs/                          architecture decisions, milestone report
src/nyc_housing_pipeline/
    config.py                  load the TOML config into dataclasses
    errors.py                  PipelineError → ConfigError | SourceError | ValidationError
    fileio.py                  sha256, atomic JSON write, JSONL append
    runtime.py                 timing, peak memory, environment versions
    bronze/                    data processing; no Dagster imports
        source.py              find file, read header, strict independent record scan
        conversion.py          Polars Lazy CSV → Parquet
        validation.py          checks + read-only diagnostics
        snapshots.py           snapshot ids, layout, latest pointer
        ingestion.py           the step-by-step flow for one dataset
    orchestration/             Dagster adapters only
        resources.py, bronze_assets.py, definitions.py
tests/                         pytest suite + byte-exact synthetic fixtures
```

Data, Parquet files, logs and Dagster state are git-ignored. Silver and Gold will
be added later as sibling packages (`silver/`, `gold/`) whose assets depend on
`bronze/<dataset>`. The existing structure won't need to change for that.

---

## Setup

```powershell
git clone <repo-url>; cd <repo>
uv sync                                   # .venv from uv.lock, dev tools included
```

## Source files

1. Download both datasets from NYC Open Data as CSV.
2. Put them in `data/source/` **unchanged**. The date suffix the portal adds to the
   filename is matched by the patterns in the config.
3. Make sure **exactly one** file matches each dataset's pattern. Zero or several
   matches stop ingestion with an error. The housing pattern names `by_Building`
   explicitly, so an old `..._by_Project_*.csv` file in the same folder is ignored.

## Run Bronze ingestion

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"      # bash: export DAGSTER_HOME="$PWD/.dagster_home"
$m = "nyc_housing_pipeline.orchestration.definitions"

uv run dagster asset materialize -m $m --select bronze/housing_production
uv run dagster asset materialize -m $m --select bronze/street_construction_permits
uv run dagster job execute -m $m -j bronze_ingestion_all        # both, in parallel
```

The housing command touches only the housing asset. The 2.9 GB permits file is not
read.

### Verify a Bronze snapshot

`data/bronze/<dataset>/latest.json` names the active snapshot. Its `manifest.json`
records:
- the dataset id (`hg8x-zxpr` for housing);
- the source filename and SHA-256;
- the record and column counts;
- the result of every check.

A quick look from the shell:

```powershell
uv run python -c "import json,polars as pl; p=json.load(open('data/bronze/housing_production/latest.json')); m=json.load(open(p['manifest_path'])); print(m['dataset'], m['source']['record_count'], m['source']['column_count'], {c['name']: c['passed'] for c in m['checks']}); print(pl.scan_parquet(p['parquet_path']).select(pl.len()).collect().item())"
```

Dagster 1.13 prints a notice that these commands are superseded by its newer
`dg` CLI. They still work.

## Dagster UI

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"
uv run dagster dev                         # http://127.0.0.1:3000
```

From the UI you can materialize one or both assets and inspect run history,
metadata (snapshot id, rows, checksums, sizes, timings, peak memory) and check
results. You can also re-execute failed runs. Re-running is always safe: unchanged
input is reused, and a failure never touches published data.

## Tests and linting

```powershell
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

---

## What Bronze guarantees

Bronze is a **faithful copy** of the source. It does no cleaning, filtering,
deduplication, imputation, type inference, date parsing or re-ordering.

| Source text | Bronze value |
| --- | --- |
| `a,,b` (unquoted empty) | `null` |
| `a,"",b` (quoted empty) | `""` |
| `"0001"`, `"0173P"`, `"13/45/2026"`, `" Brooklyn "`, `"NA"` | unchanged strings |
| quoted commas, embedded newlines, `""` escapes, Unicode | preserved |
| duplicate rows, odd identifiers | kept; reported as warnings |

**Malformed input stops ingestion.** Polars on its own would silently pad short
rows, rename duplicate headers and turn blank lines into all-null rows. Before
converting, the pipeline therefore parses every record with an independent strict
parser (Python's `csv`). Wrong field counts, broken quoting, blank lines, bad
header names or invalid UTF-8 stop ingestion with the record number and line, and
nothing is published.

## Checks (Dagster asset checks)

| Check | Blocking | Verifies |
| --- | --- | --- |
| `source_unchanged` | yes | source size and mtime identical before and after conversion |
| `schema_matches_source` | yes | same column names and order as the header; all String |
| `row_count_matches_source` | yes | Bronze rows = independently counted records (nothing silently dropped) |
| `sample_values_match_source` | yes | ~1,000 random records plus the last one match value for value, null vs `""` included |
| `schema_matches_contract` | warn | extra or re-ordered columns vs the config (kept as-is) |
| `no_duplicate_rows` | warn | exact duplicate rows (kept) |
| `identifiers_unique` | warn | nulls, empty strings or repeats in identifier columns (kept) |

If the source is missing a configured column, ingestion fails.

## Snapshots

- `snapshot_id = <16 hex of source SHA-256>_<8 hex of the settings fingerprint>`.
  The same bytes with the same settings always give the same snapshot.
- **Idempotent:** a re-run on an unchanged source verifies the existing snapshot's
  checksum and reuses it, without rewriting anything.
- **New source bytes** create a new snapshot next to the old ones. `latest.json`
  points to the most recently ingested snapshot.
- **Atomic:** output is written and validated in `_staging_<run_id>/`, then
  published with a single directory rename. On any failure the staging folder is
  deleted, and existing snapshots and `latest.json` are not touched.
- **Damaged snapshots** (checksum mismatch) cause an error. They are never repaired
  or deleted automatically.
- **Manifest** fields:
  - dataset id;
  - source: filename, path, size, SHA-256, record count, columns;
  - output: path, size, SHA-256, rows, compression ratio;
  - parse settings;
  - UTC start and finish times, per-step timings, peak memory;
  - all check results, diagnostics, and library versions.

---

## Implemented

Repository and tooling · config-driven source discovery and column contract ·
independent strict source verification · streaming Polars Lazy CSV → Parquet ·
blocking validation and report-only diagnostics · content-addressed, idempotent,
atomic snapshots with manifests · run log · Dagster assets, checks, lineage and a job ·
78 automated tests.

## Not implemented (intentionally)

Silver cleaning · Gold models · joining the datasets · research analyses ·
visualizations · website · schedules and sensors · automated or incremental
downloads · partitioned Parquet · cloud storage and deployment · streaming.
