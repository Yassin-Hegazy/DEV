"""Bronze snapshot layout and identity.

    <bronze_dir>/<dataset>/snapshots/<snapshot_id>/data.parquet
    <bronze_dir>/<dataset>/snapshots/<snapshot_id>/manifest.json
    <bronze_dir>/<dataset>/latest.json            pointer to the latest ingested snapshot

snapshot_id = <16 hex of source SHA-256>_<8 hex of settings fingerprint>, so the same
source bytes with the same settings always land in the same snapshot (idempotent).
"""

import hashlib
import json
from pathlib import Path
from typing import Any

from nyc_housing_pipeline.errors import ValidationError
from nyc_housing_pipeline.fileio import read_json, sha256_file, write_json

PARQUET_FILE = "data.parquet"
MANIFEST_FILE = "manifest.json"
LATEST_FILE = "latest.json"


def snapshot_id(source_sha256: str, fingerprint: dict[str, Any]) -> str:
    settings = json.dumps(fingerprint, sort_keys=True).encode()
    return f"{source_sha256[:16]}_{hashlib.sha256(settings).hexdigest()[:8]}"


def snapshot_dir(bronze_dir: Path, dataset_key: str, snapshot: str) -> Path:
    return bronze_dir / dataset_key / "snapshots" / snapshot


def staging_dir(bronze_dir: Path, dataset_key: str, run_id: str) -> Path:
    """Private work directory on the same disk, so publishing is one atomic rename."""
    return bronze_dir / dataset_key / f"_staging_{run_id}"


def load_snapshot(directory: Path) -> dict[str, Any] | None:
    """Manifest of an intact published snapshot, or None if it does not exist.

    A damaged snapshot raises: it is never repaired or deleted automatically.
    """
    if not directory.exists():
        return None
    try:
        manifest = read_json(directory / MANIFEST_FILE)
        intact = sha256_file(directory / PARQUET_FILE) == manifest["output"]["sha256"]
    except (OSError, KeyError, ValueError) as exc:
        raise ValidationError(f"Snapshot {directory} is incomplete or unreadable: {exc}") from exc
    if not intact:
        raise ValidationError(
            f"Snapshot {directory} is corrupted (Parquet checksum differs from its manifest). "
            "Inspect it and delete the directory by hand to re-ingest."
        )
    return manifest


def write_latest(bronze_dir: Path, dataset_key: str, pointer: dict[str, Any]) -> None:
    write_json(bronze_dir / dataset_key / LATEST_FILE, pointer)


def read_latest(bronze_dir: Path, dataset_key: str) -> dict[str, Any] | None:
    path = bronze_dir / dataset_key / LATEST_FILE
    return read_json(path) if path.exists() else None
