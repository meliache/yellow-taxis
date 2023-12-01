"""Utility functions for use in Luigi tasks."""

from os import PathLike
from pathlib import Path

from joblib import Memory
from xdg_base_dirs import xdg_cache_home

# persistent on-diskmemory cache
cache_dir = xdg_cache_home() / "yellow-taxis"
memory = Memory(cache_dir, verbose=0)


def estimate_memory_usage_mb(
    parquet_path: PathLike,
    compression_factor_estimate: float = 1.5,
    safety_factor: float = 2.0,
) -> float:
    """Estimate memory usage of parquet dataframe in MB when loaded as a pandas df.

    The default compression factor estimate has been determined by comparing the pandas
    in-memory size and the on-disk size of a couple of example dataframes, but it varies
    greatly, therefore with it and the safety factor I try to err on the conservative
    site.

    :param compression_factor_estimate: Compression factor estimate.
    :param paquet_path: File path to paquet file.
    :return: Memory usage estimate in MB.
    """
    on_disk_size = Path(parquet_path).stat().st_size / 1e6
    return on_disk_size * compression_factor_estimate * safety_factor


def year_month_result_dir(
    result_dir: PathLike, year: int, month: int, make_parents: bool = True
) -> Path:
    """Return a result directory for a specific year and month.

    :param result_dir: Root directory where results are stored. Should be the same for
        all tasks.
    :param year: Year of dataset.
    :param month: Month of dataset.
    :param make_parents: If ``true`` create parent directories.
    """
    path = Path(result_dir) / f"{year:d}" / f"{month:02d}"
    if make_parents:
        path.parent.mkdir(parents=True, exist_ok=True)
    return path
