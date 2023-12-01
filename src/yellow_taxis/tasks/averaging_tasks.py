#!/usr/bin/env python3

from os import PathLike
from pathlib import Path
from typing import Any

import luigi
import pandas as pd
from dateutil.relativedelta import relativedelta
from joblib import Memory
from luigi.util import requires
from xdg_base_dirs import xdg_cache_home

from yellow_taxis import fetch
from yellow_taxis.tasks.download_task import (
    RESULT_DIR,
    DownloadTask,
    year_month_result_dir,
)

# Required columns. These are from the documented schema, since 2015
COLUMN_NAMES = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]

# Historical data from 2015 and earlier uses different column name conventions.
COLUMN_NAME_VARIATIONS = (
    COLUMN_NAMES,
    ["Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Trip_Distance"],
    ["pickup_datetime", "dropoff_datetime", "trip_distance"],
)


def read_taxi_dataframe(file_name: PathLike) -> pd.DataFrame:
    """Read parquet file with NYC yellow-taxi data into dataframe.

    Normalizes also all data to the same schema type with timestamp-colums of datetime
    type.

    :param file_name: Input file parquet file path.
    :return: Pandas dataframe with (normalized to latest format).
    """
    # handle different historical input schemas
    for columns in COLUMN_NAME_VARIATIONS:
        try:
            df = pd.read_parquet(file_name, columns=columns)
            df.columns = COLUMN_NAMES

            # ensure datetime columns are of date type and not just strings
            time_format = "%Y-%m-%d %H:%M:%S"
            for time_col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                df[time_col] = pd.to_datetime(df[time_col], format=time_format)

            return df

        except ValueError:  # try different column set
            pass

    raise ValueError(
        f"Parquet file contains none of the column sets {COLUMN_NAME_VARIATIONS}!"
    )


# persistent on-diskmemory cache
cache_dir = xdg_cache_home() / "yellow-taxis"
memory = Memory(cache_dir, verbose=0)


@memory.cache
def data_memory_usage_mb(
    parquet_path: PathLike,
) -> float:
    """Pandas memory usage of parquet file in MB when read as a pandas dataframe.

    On first invocation this will be slow due to requiring loading the dataset, but due
    to persistent caching faster on subsequent invocations. This is useful for
    determining required memory resources.

    :param paquet_path: File path to paquet file.
    :return: Memory usage in MB.
    """
    # Initially I tried just multiplying the on-disk file size with a
    # compression-factor, but due to different schemas the compression ratios vary
    # widely over historical datasets and might differ based on filesystem.
    return pd.read_parquet(parquet_path).size / 1e6


@requires(DownloadTask)
class MonthlyAveragesTask(luigi.Task):
    # Will uses this for the name the single column dataframe that this task generates.
    # When concatenating results this will give us a date string index. Would have
    # preferred a datetime object but parquet only allows for string column names.
    # Having this a property is important for accessing this in other tasks.
    month_date_fmt = "%Y-%m"

    @property
    def result_path(self) -> Path:
        return (
            year_month_result_dir(self.result_dir, self.year, self.month)
            / "month_average.parquet"
        )

    @property
    def resources(self) -> dict[str, Any]:
        return {
            "cpus": 1,
            "memory": data_memory_usage_mb(self.input.Path()),
        }

    def run(self):
        input_fpath = Path(self.input().path)
        df = read_taxi_dataframe(input_fpath)

        durations = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        df["trip_duration"] = durations.dt.seconds

        results: dict[str, float] = {}
        for col in ["trip_distance", "trip_duration"]:
            # calculate mean and uncertainty on mean
            results[f"{col}_mean"] = df[col].mean()
            results[f"{col}_mean_err"] = df[col].sem()

        result_series = pd.Series(results)
        col_name = pd.Timestamp(self.year, self.month, 1).strftime(self.month_date_fmt)
        result_df = result_series.to_frame(col_name)
        result_df.to_parquet(self.result_path)

    def output(self):
        return luigi.LocalTarget(self.result_path)

    result_dir = luigi.PathParameter(absolute=True)


class RollingAveragesTask(luigi.Task):
    """Calculate the 45 day rolling averages for all dates in a given month."""

    result_dir = luigi.PathParameter(absolute=True)

    ndays = luigi.IntParameter(default=45)
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        this_month_start_date = pd.Timestamp(self.year, self.month, 1)

        # we need the data of the current, previous and previous of the previous month
        for neg_months_delta in range(3):
            _date = this_month_start_date - relativedelta(months=neg_months_delta)
            yield self.clone(
                DownloadTask,
                year=_date.year,
                month=_date.month,
            )

    @property
    def result_path(self) -> Path:
        return (
            year_month_result_dir(self.result_dir, self.year, self.month)
            / "rolling_averages.parquet"
        )

    def run(self):
        input_fpaths = [Path(input_target.path) for input_target in self.input()]
        assert len(input_fpaths == 3)

        df = pd.concat([pd.read_parquet(f) for f in input_fpaths])

        durations = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        df["trip_duration"] = durations.dt.seconds

        results: dict[str, float] = {}
        for col in ["trip_distance", "trip_duration"]:
            # calculate mean and uncertainty on mean
            results[f"{col}_mean"] = df[col].mean()
            results[f"{col}_mean_err"] = df[col].sem()

        result_series = pd.Series(results)
        result_series.to_json(self.result_path)

    def output(self):
        return luigi.LocalTarget(self.result_path)


class AggregateAveragesTask(luigi.Task):
    result_dir = luigi.PathParameter(absolute=True)

    @property
    def averages_fname(self):
        return self.result_dir / "monthly_averages.parquet"

    resources = {"cpus": 1}

    def requires(self):
        for date in fetch.available_dataset_dates():
            yield self.clone(
                MonthlyAveragesTask,
                year=date.year,
                month=date.month,
            )

    def run(self):
        monthly_averages: pd.DataFrame = pd.concat(
            [pd.read_parquet(input_target.path) for input_target in self.input()],
            axis=1,
        ).T
        monthly_averages.index = pd.to_datetime(
            monthly_averages.index, format=MonthlyAveragesTask.month_date_fmt
        )
        monthly_averages.to_parquet(self.averages_fname)

    def output(self):
        return self.averages_fname


def run_locally() -> None:
    luigi.build(
        [AggregateAveragesTask(result_dir=RESULT_DIR)], local_scheduler=True, workers=1
    )


if __name__ == "__main__":
    run_locally()
