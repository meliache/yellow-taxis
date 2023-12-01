"""Utility functions for use in Luigi tasks."""

from os import PathLike
from pathlib import Path


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