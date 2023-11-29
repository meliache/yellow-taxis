"""
Module for downloading parquet files with monthly trip data from
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
"""

import os
import shutil
import tempfile
from pathlib import Path

import requests
import validators

#: format string that given a year and month and can be formatted to a valid parquet
# download URL.
URL_FORMAT_STRING: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:d}-{month:02d}.parquet"


def dataset_url(
    year: int,
    month: int,
) -> str:
    """Return URL for file with yellow taxis trip data.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded, as integer from 1.
    :return: Download URL for parquet file with monthly trip data
    """
    if not isinstance(year, int):
        raise TypeError(f"Year must be an integer but if {year=}")

    if not isinstance(month, int):
        raise TypeError(f"Month must be an integer but if {month=}")

    if not 1 <= month <= 12:
        raise ValueError(
            f"Invalid month, it should be an integer from 1 to 12 but is {month}."
        )
    if year < 2009:
        raise ValueError(f"Historical data for {year} does not exist.")

    return URL_FORMAT_STRING.format(year=year, month=month)


def dataset_exists(year, month) -> bool:
    """Check if we can find a dataset on the website the given year and month.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded, as integer from 1

    :return: ``True`` if dataset for this date and time exists under the URL.
    """

    url: str = dataset_url(year, month)
    if requests.head(url).status_code == 200:
        return True
    return False


def download(
    url: str,
    file_name: os.PathLike,
    make_directories: bool = True,
    overwrite: bool = False,
) -> None:
    """Download data from `url` to `file_name`.

    :param url: Data URL
    :param file_name: File name
    :make_directories: Create parent directories
    :overwrite: If ``True``, overwrite ``file_name`` if it exists, otherwise fail.
    """
    if not validators.url(url):
        raise ValueError(f"ULR '{url}' is invalid!")

    # Download code adapted from https://stackoverflow.com/a/39217788/6199035
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        # download to temporary filename first to avoid partially downloaded data in
        # case of download failure
        with tempfile.NamedTemporaryFile("wb", delete=False) as tmp_file:
            shutil.copyfileobj(r.raw, tmp_file)

            if make_directories:
                Path(file_name).parent.mkdir(parents=True, exist_ok=True)

            shutil.move(tmp_file.name, file_name)


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
    download(url, file_name, make_directories=make_directories)
