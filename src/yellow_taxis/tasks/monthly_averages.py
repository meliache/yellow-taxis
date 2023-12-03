#!/usr/bin/env python3

import math
from pathlib import Path

import luigi
import pandas as pd
from luigi.util import requires
from pandas.api.types import is_datetime64_any_dtype as is_datetime

from yellow_taxis import fetch
from yellow_taxis.dataframe_utils import (
    add_trip_duration,
    read_taxi_dataframe,
    reject_not_in_month,
    reject_outliers,
    rolling_means,
)
from yellow_taxis.task_utils import year_month_result_dir
from yellow_taxis.tasks.download import RESULT_DIR, DownloadTask


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

    resources = {
        "cpus": 1,
        "memory": 4_000,
    }

    def run(self):
        input_fpath = Path(self.input().path)

        df = read_taxi_dataframe(input_fpath)
        df = reject_not_in_month(df, self.year, self.month, on="tpep_dropoff_datetime")
        df = add_trip_duration(df)
        df = reject_outliers(df)

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


class AggregateMonthlyAveragesTask(luigi.Task):
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


class RollingAveragesTask(luigi.Task):
    """Calculate the 45 day rolling averages for all dates in a given month."""

    result_dir = luigi.PathParameter(absolute=True)

    window = luigi.IntParameter(default=45)

    @property
    def n_months_required(self) -> int:
        """How many data files needed to be loaded simultaneously for the given time
        window.

        E.g. 3 months for the default of a 45 day window, because for the first of the
        month we need the previous and the previous of the previous month.
        """
        return math.ceil(self.window / 30) + 1

    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def result_path(self) -> Path:
        return (
            year_month_result_dir(self.result_dir, self.year, self.month)
            / "rolling_averages.parquet"
        )

    resources = {
        "cpus": 1,
        "memory": 4_000,
    }

    def _months_required(self) -> list[pd.Timestamp]:
        """Return timestamps of the 3 months for calculating this rolling average.

        The day is always set to to first of the month.
        """
        this_month_start_date = pd.Timestamp(self.year, self.month, 1)
        # we need the data of the current, previous and previous of the previous month
        month_dates: list[pd.Timestamp] = []

        for neg_months_delta in range(self.n_months_required):
            _date = this_month_start_date - pd.tseries.offsets.MonthBegin(
                neg_months_delta
            )
            # usually we require previous n months but if the month would be before the
            # first historical record then we cannot use those months' data
            if _date < fetch.DATE_FIRST_RECORDS:
                break
            month_dates.append(_date)

        return month_dates

    def requires(self):
        for _date in self._months_required():
            yield self.clone(
                DownloadTask,
                year=_date.year,
                month=_date.month,
            )

    def _reject_not_in_range(self, data: pd.DataFrame, on: str | None) -> pd.DataFrame:
        """Reject trip data not in month range of the used datasets.

        :param data: Trip data dataframe, concatenated from the trip data of several
            months to allow for calculating the rolling average for the current month.
        :param on: Dataframe column name to determine datetime of trip. If ``None``
           use dataframe index.
        """
        this_month_begin = pd.Timestamp(self.year, self.month, 1)
        next_month_begin = this_month_begin + pd.offsets.MonthBegin(1)
        oldest_month_begin = this_month_begin - pd.offsets.MonthBegin(
            self.n_months_required - 1
        )

        date = data[on] if on else data.index
        if not is_datetime(date):
            raise ValueError(f"Date should be a datetime but is type {date.dtype}!")

        return data[(date > oldest_month_begin) & (date < next_month_begin)]

    def run(self):
        df = pd.concat(
            [read_taxi_dataframe(target.path) for target in self.input()],
            ignore_index=True,
        )
        df = add_trip_duration(df)
        df = reject_outliers(df)

        df.set_index("tpep_dropoff_datetime", inplace=True, drop=True)
        df = self._reject_not_in_range(df)

        this_month_begin = pd.Timestamp(self.year, self.month, 1)
        rolling_means_this_month = rolling_means(
            df, n_window_days=self.window, keep_after=this_month_begin
        )

        rolling_means_this_month.to_parquet(self.result_path)

    def output(self):
        return luigi.LocalTarget(self.result_path)


class RollingAverageTasksWrapper(luigi.WrapperTask):
    result_dir = luigi.PathParameter(absolute=True)

    resources = {"cpus": 1}

    def requires(self):
        for date in fetch.available_dataset_dates():
            yield self.clone(
                RollingAveragesTask,
                year=date.year,
                month=date.month,
            )


def run_locally() -> None:
    luigi.build(
        [
            AggregateMonthlyAveragesTask(result_dir=RESULT_DIR),
        ],
        local_scheduler=True,
        workers=1,
    )


if __name__ == "__main__":
    run_locally()