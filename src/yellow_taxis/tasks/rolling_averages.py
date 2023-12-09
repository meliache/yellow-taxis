#!/usr/bin/env python3

import datetime
import math
from pathlib import Path

import luigi
import pandas as pd
import polars as pl
from dateutil.relativedelta import relativedelta
from dateutil.rrule import MONTHLY, rrule

from yellow_taxis import fetch
from yellow_taxis.dataframe_utils import (
    add_trip_duration,
    read_taxi_dataframe,
    reject_outliers,
    rolling_means,
)
from yellow_taxis.settings import get_settings
from yellow_taxis.task_utils import TaxiBaseTask
from yellow_taxis.tasks.download import DownloadTask


class RollingAveragesTask(TaxiBaseTask):
    """Calculate the 45 day rolling averages for all dates in a given month."""

    output_base_name = Path("rolling_averages.parquet")

    default_window: int | None = get_settings().get("rolling_window")
    window = luigi.IntParameter(
        default=default_window,
        description="Number of days to use for the window of the rolling average.",
    )

    month_date = luigi.MonthParameter(
        description="Date of month for which to calculate the rolling averages."
    )

    @property
    def n_months_required(self) -> int:
        """How many data files needed to be loaded simultaneously for the given
        time window.

        E.g. 3 months for the default of a 45 day window, because for
        the first of the month we need the previous and the previous of
        the previous month.
        """
        min_month_days = 28  # February
        month_required_for_full_window = math.ceil(self.window / min_month_days) + 1

        # if we are close to start of first records we can only require less months
        n_months_since_first_records = len(
            list(
                rrule(MONTHLY, dtstart=fetch.DATE_FIRST_RECORDS, until=self.month_date)
            )
        )

        return min(month_required_for_full_window, n_months_since_first_records)

    resources = {
        "cpus": 1,
        "memory": 4_000,
    }

    def _months_required(self) -> list[datetime.date]:
        """Return timestamps of the 3 months for calculating this rolling
        average.

        The day is always set to to first of the month.
        """
        # we need the data of the current, previous and previous of the previous month
        month_dates: list[datetime.date] = []

        for neg_months_delta in range(self.n_months_required):
            _date = self.month_date - relativedelta(months=neg_months_delta)
            month_dates.append(_date)

        return month_dates

    def requires(self):
        """Require data all month data needed for rolling average.

        This will be current months and the previous available months
        required for the given day window.
        """
        for _date in self._months_required():
            yield self.clone(
                DownloadTask,
                month_date=_date,
            )

    def _reject_not_in_range(self, data: pl.DataFrame, on: str) -> pl.DataFrame:
        """Reject trip data not in month range of the used datasets.

        :param data: Trip data dataframe, concatenated from the trip
            data of several months to allow for calculating the rolling
            average for the current month.
        :param on: Dataframe column name to determine datetime of trip.
        """
        next_month_begin = self.month_date + relativedelta(months=1)
        oldest_month_begin = min(self._months_required())
        date = data[on]
        if not isinstance(date, pl.Datetime):
            raise ValueError(f"Date should be a datetime but is type {date.dtype}!")

        in_range_data = data.filter(
            (date >= oldest_month_begin) & (date < next_month_begin)
        )
        if len(in_range_data) == 0:
            raise RuntimeError(
                f"No trips between {oldest_month_begin} and {next_month_begin}!"
            )
        return in_range_data

    def run(self):
        """Calculate the rolling averages for the month."""
        df = pl.concat(
            [read_taxi_dataframe(target.path) for target in self.input()],
        )
        df = add_trip_duration(df)
        df = reject_outliers(
            df, max_duration_s=self.max_duration, max_distance=self.max_distance
        )
        df = df.sort("tpep_dropoff_datetime")
        df = self._reject_not_in_range(df, on="tpep_dropoff_datetime")
        rolling_means_by_trip = rolling_means(
            df,
            n_window_days=self.window,
            on="tpep_dropoff_datetime",
            keep_after=self.month_date,
        )
        # Only keep the last rolling mean of each day.
        # Massively reduces output data size
        # TODO
        rolling_means_by_day = rolling_means_by_trip.resample(
            pd.Timedelta(days=1)
        ).last()

        with self.output().temporary_path() as self.temp_output_path:
            # compress rolling averages by default to zstd
            # because they take up a lot of disk space
            rolling_means_by_day.to_parquet(self.temp_output_path, compression="zstd")


class AggregateRollingAveragesTask(TaxiBaseTask):
    """Task to collect running averages from months."""

    output_base_name = Path("all_month_rolling_averages.parquet")

    # Month parameter for most recent available dataset. This will be encoded in the
    # output and thus force the pipeline to be re-run if a new dataset gets published.
    last_month = luigi.MonthParameter(
        default=fetch.most_recent_dataset_date(),
        description="Most recent month for which a dataset is available",
    )

    default_window: int | None = get_settings().get("rolling_window")
    window = luigi.IntParameter(
        default=default_window,
        description="Number of days to use for the window of the rolling average.",
    )

    resources = {"cpus": 1, "memory": 1000}

    def requires(self):
        """Require rolling averages for all months."""
        for date in fetch.available_dataset_dates(self.last_month):
            yield self.clone(RollingAveragesTask, month_date=date)

    def run(self):
        """Collect running averages from all months in single data frame."""
        running_averages = pd.concat(
            [pd.read_parquet(input_target.path) for input_target in self.input()]
        )
        with self.output().temporary_path() as self.temp_output_path:
            running_averages.to_parquet(self.temp_output_path, compression="zstd")


def run_locally() -> None:
    """Run pipeline for rolling averages locally."""
    luigi.build(
        [
            AggregateRollingAveragesTask(),
        ],
        local_scheduler=True,
        workers=1,
    )


if __name__ == "__main__":
    run_locally()
