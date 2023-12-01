#!/usr/bin/env python3

from pathlib import Path
from typing import Any

import luigi
import pandas as pd
from dateutil.relativedelta import relativedelta
from luigi.util import requires

from yellow_taxis import fetch
from yellow_taxis.dataframe_utils import read_taxi_dataframe
from yellow_taxis.task_utils import (
    MEMORY_RESOURCE_SAFETY_FACTOR,
    data_memory_usage_mb,
    year_month_result_dir,
)
from yellow_taxis.tasks.download_task import RESULT_DIR, DownloadTask


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
        request_memory = (
            data_memory_usage_mb(self.input.Path()) * MEMORY_RESOURCE_SAFETY_FACTOR
        )
        return {
            "cpus": 1,
            "memory": request_memory,
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

    @property
    def result_path(self) -> Path:
        return (
            year_month_result_dir(self.result_dir, self.year, self.month)
            / "rolling_averages.parquet"
        )

    @property
    def resources(self) -> dict[str, Any]:
        request_memory = (
            sum(data_memory_usage_mb(inp.path for inp in self.input()))
            * MEMORY_RESOURCE_SAFETY_FACTOR
        )
        return {
            "cpus": 1,
            "memory": request_memory,
        }

    def requires(self):
        this_month_start_date = pd.Timestamp(self.year, self.month, 1)

        # we need the data of the current, previous and previous of the previous month
        n_months_required = 3
        for neg_months_delta in range(n_months_required):
            _date = this_month_start_date - relativedelta(months=neg_months_delta)
            yield self.clone(
                DownloadTask,
                year=_date.year,
                month=_date.month,
            )

    def run(self):
        df = pd.concat(
            [read_taxi_dataframe(target) for target in self.input()], ignore_index=True
        )

        durations = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        df["trip_duration"] = durations.dt.seconds

        df.set_index("tpep_dropoff_datetime", inplace=True)
        df.sort_index(inplace=True)

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
