"""
Module for downloading parquet files with monthly trip data from
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
"""

import datetime
import os
import shutil
import subprocess
import time
from functools import cache
from pathlib import Path

import requests
import validators
from dateutil.relativedelta import relativedelta
from dateutil.rrule import MONTHLY, rrule

#: format string that given a year and month and can be formatted to a valid parquet
# download URL.
URL_FORMAT_STRING = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:d}-{month:02d}.parquet"

#: Year for which we have the first records
DATE_FIRST_RECORDS = datetime.date(year=2009, month=1, day=1)


def dataset_url(date: datetime.date) -> str:
    """Return URL for file with yellow taxis trip data.

    :param date: Datetime for year and month of dataset.
    :return: Download URL for parquet file with monthly trip data
    """
    if date != date.replace(day=1):
        raise ValueError(f"{date=} should be beginning of month!")
    if date < DATE_FIRST_RECORDS:
        raise ValueError(f"Historical data for {date=} does not exist.")

    return URL_FORMAT_STRING.format(year=date.year, month=date.month)


def dataset_exists(date) -> bool:
    """Check if we can find a dataset on the website for given date.

    :param date: Datetime for year and month of dataset.
    :return: ``True`` if dataset for this date and time exists under the URL.
    """
    if date > datetime.date.today():  # this is in the future so can't exist
        return False

    url: str = dataset_url(date)
    if requests.head(url).status_code == 200:
        return True

    return False


@cache
def most_recent_dataset_date() -> datetime.date:
    """Get datetime of the most recent month for which there is taxi data."""

    # It first sends an HTTPS request for the predicted dataset URL for the current
    # month, to check whether it is available. If it's not available, try previous
    # month, and so on… If we get to oldest dataset date (2009, raise error.)
    date = datetime.date.today().replace(day=1)
    while not dataset_exists(date):
        date -= relativedelta(months=1)
        if date < DATE_FIRST_RECORDS:
            raise RuntimeError("Could not find any datasets.")
        time.sleep(1)  # avoid DDOS'ing server
    return date


def available_dataset_dates(
    most_recent_month: datetime.date | None = None,
) -> list[datetime.date]:
    """List of dates of all available parquet files.

    :param most_recent_month: Datetime of 1st day of the most recent
        month for which a taxi data dataframe is available on the
        website.
    :return: Sorted list of month datetimes between first records in
        2009 and the most recent available dataset date. Always set to
        the beginning of the month.
    """
    if most_recent_month is None:
        most_recent_month = most_recent_dataset_date()
    dates = [
        dt.date()
        for dt in rrule(MONTHLY, dtstart=DATE_FIRST_RECORDS, until=most_recent_month)
    ]
    return dates


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
    date: datetime.date,
    file_name: os.PathLike,
    make_directories: bool = True,
    overwrite: bool = False,
) -> None:
    """Download yellow taxis trip data.

    :param date: Datetime for year and month of dataset (beginning of month.)
    :param url: Data URL
    :param file_name: File name
    :make_directories: Create parent directories
    :overwrite: If ``True``, overwrite ``file_name`` if it exists.
    """
    url: str = dataset_url(date=date)
    download(url, file_name, make_directories=make_directories, overwrite=overwrite)
