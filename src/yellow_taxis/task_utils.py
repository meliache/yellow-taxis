"""Utility functions for use in Luigi tasks."""

from os import PathLike
from pathlib import Path

import pandas as pd
from joblib import Memory
from xdg_base_dirs import xdg_cache_home

# persistent on-diskmemory cache
cache_dir = xdg_cache_home() / "yellow-taxis"
memory = Memory(cache_dir, verbose=0)

# If a dataframe has a certain size in memory, how much more resources should we request
# to allow for addiataion memory usage durationg computations
MEMORY_RESOURCE_SAFETY_FACTOR: float = 2.0


@memory.cache
def data_memory_usage_mb(
    parquet_path: PathLike,
) -> float:
    """Pandas memory usage of parquet file in MB when read as a pandas dataframe.

    On first invocation this will be slow due to requiring loading the dataset, but due
    to persistent caching faster on subsequent invocations. This is useful for
    determining required memory resources for luigi tasks.

    :param paquet_path: File path to paquet file.
    :return: Memory usage in MB.
    """
    # Initially I tried just multiplying the on-disk file size with a
    # compression-factor, but due to different schemas the compression ratios vary
    # widely over historical datasets and might differ based on filesystem.
    return pd.read_parquet(parquet_path).size / 1e6


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
