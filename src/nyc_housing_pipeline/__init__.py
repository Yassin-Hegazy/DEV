"""NYC affordable-housing and street-construction data engineering pipeline."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("nyc-housing-pipeline")
except PackageNotFoundError:  # pragma: no cover - running from an uninstalled tree
    __version__ = "0.0.0+unknown"
