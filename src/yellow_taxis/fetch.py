"""
Module for downloading parquet files with monthly trip data from
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
"""

import os
import shutil
import subprocess
import time
from functools import cache
from pathlib import Path

import pandas as pd
import requests
import validators

#: format string that given a year and month and can be formatted to a valid parquet
# download URL.
URL_FORMAT_STRING = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:d}-{month:02d}.parquet"

#: Year for which we have the first records
DATE_FIRST_RECORDS = pd.Timestamp(year=2009, month=1, day=1)


def _validate_date(year: int, month: int):
    """Validate year and month as possible date to consider for yellow taxi
    data.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded, as integer from
        1.
    """
    if not isinstance(year, int):
        raise TypeError(f"Year must be an integer but if {year=}")

    if not isinstance(month, int):
        raise TypeError(f"Month must be an integer but if {month=}")

    if not 1 <= month <= 12:
        raise ValueError(
            f"Invalid month, it should be an integer from 1 to 12 but is {month}."
        )

    date = pd.Timestamp(year, month, 1)  # utilizes ``pd.Timestamp`` validation
    # Check date is not before first records
    if date < DATE_FIRST_RECORDS:
        raise ValueError(f"Historical data for {year} does not exist.")


def dataset_url(
    year: int,
    month: int,
) -> str:
    """Return URL for file with yellow taxis trip data.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded, as integer from 1.
    :return: Download URL for parquet file with monthly trip data
    """
    _validate_date(year, month)
    return URL_FORMAT_STRING.format(year=year, month=month)


def dataset_exists(year, month) -> bool:
    """Check if we can find a dataset on the website fort the given year and
    month.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded, as integer from 1

    :return: ``True`` if dataset for this date and time exists under the URL.
    """
    _validate_date(year, month)

    date = pd.Timestamp(year, month, 1)

    if date > pd.Timestamp.now():  # this is in the future so can't exist
        return False

    url: str = dataset_url(year, month)
    if requests.head(url).status_code == 200:
        return True

    return False


@cache
def most_recent_dataset_date() -> pd.Timestamp:
    date = pd.Timestamp(
        year=pd.Timestamp.today().year, month=pd.Timestamp.today().month, day=1
    )
    while not dataset_exists(date.year, date.month):
        date -= pd.tseries.offsets.MonthBegin(1)
        if date < DATE_FIRST_RECORDS:
            raise RuntimeError("Could not find any datasets.")
        time.sleep(1)  # avoid DDOS'ing server
    return date


def available_dataset_dates() -> list[pd.Timestamp]:
    """List of dates of all available parquet files."""

    dates = []
    _date = DATE_FIRST_RECORDS
    while DATE_FIRST_RECORDS <= _date <= most_recent_dataset_date():
        dates.append(_date)
        _date += pd.tseries.offsets.MonthBegin(1)
    return list(sorted(dates))


def download(
    url: str,
    file_name: os.PathLike,
    make_directories: bool = True,
    overwrite: bool = False,
) -> None:
    """Download data from ``url`` to ``file_name``.

    First download to ``<file_name>.partial`` before moving to file name
    to solve the atomic-writes problem.

    :param url: Data URL
    :param file_name: File name
    :make_directories: Create parent directories
    :overwrite: If ``True``, overwrite ``file_name`` if it exists, otherwise fail.
    """
    file_name = Path(file_name)
    if file_name.exists():
        raise FileExistsError(f"{file_name} already exists!")

    if not validators.url(url):
        raise ValueError(f"ULR '{url}' is invalid!")

    if make_directories:
        file_name.parent.mkdir(parents=True, exist_ok=True)

    # download to temporary/partial file name at first, only mv to final name on success
    partial_file_name = file_name.with_suffix(".partial")

    # Downloading with ``curl``. I wanted to use ``requests`` initially, but for large
    # files it was slower and less robust than just curl even when streaming.
    print(f"Downloading {url} to {partial_file_name}…")
    dowload_cmd = [
        "curl",
        "--location",
        url,
        "--output",
        str(partial_file_name),
    ]
    subprocess.run(dowload_cmd, check=True, capture_output=False)
    shutil.move(partial_file_name, file_name)


def download_monthly_data(
    year: int,
    month: int,
    file_name: os.PathLike,
    make_directories: bool = True,
    overwrite: bool = False,
) -> None:
    """Download yellow taxis trip data.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded as integer from 1.
    :param url: Data URL
    :param file_name: File name
    :make_directories: Create parent directories
    :overwrite: If ``True``, overwrite ``file_name`` if it exists.
    """
    url: str = dataset_url(year=year, month=month)
    download(url, file_name, make_directories=make_directories, overwrite=overwrite)
