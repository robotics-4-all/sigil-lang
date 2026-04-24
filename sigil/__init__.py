"""Sigil -- DSL for stream-processing aggregation pipelines."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("sigil")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0+local"

__all__ = ["__version__"]
