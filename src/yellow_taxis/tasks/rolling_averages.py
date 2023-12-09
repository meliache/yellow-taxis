#!/usr/bin/env python3

import math
from pathlib import Path

import luigi
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime

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

    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def n_months_required(self) -> int:
        """How many data files needed to be loaded simultaneously for the given
        time window.

        E.g. 3 months for the default of a 45 day window, because for
        the first of the month we need the previous and the previous of
        the previous month.
        """

        month_required_for_full_window = math.ceil(self.window / 30) + 1

        # if we are close to start of first records we can only require less months
        month_first_records = fetch.DATE_FIRST_RECORDS.to_period(freq="M")
        current_month = pd.Period(year=self.year, month=self.month, freq="M")
        n_months_since_first_records = (current_month - month_first_records).n

        return min(month_required_for_full_window, n_months_since_first_records + 1)

    resources = {
        "cpus": 1,
        "memory": 4_000,
    }

    def _months_required(self) -> list[pd.Timestamp]:
        """Return timestamps of the 3 months for calculating this rolling
        average.

        The day is always set to to first of the month.
        """
        this_month_start_date = pd.Timestamp(self.year, self.month, 1)
        # we need the data of the current, previous and previous of the previous month
        month_dates: list[pd.Timestamp] = []

        for neg_months_delta in range(self.n_months_required):
            _date = this_month_start_date - pd.tseries.offsets.MonthBegin(
                neg_months_delta
            )
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
                year=_date.year,
                month=_date.month,
            )

    def _reject_not_in_range(
        self, data: pd.DataFrame, on: str | None = None
    ) -> pd.DataFrame:
        """Reject trip data not in month range of the used datasets.

        :param data: Trip data dataframe, concatenated from the trip data of several
            months to allow for calculating the rolling average for the current month.
        :param on: Dataframe column name to determine datetime of trip. If ``None``
           use dataframe index.
        """
        this_month_begin = pd.Timestamp(self.year, self.month, 1)
        next_month_begin = this_month_begin + pd.offsets.MonthBegin(1)
        oldest_month_begin: pd.Timestamp = min(self._months_required())
        date = data[on] if on else data.index
        if not is_datetime(date):
            raise ValueError(f"Date should be a datetime but is type {date.dtype}!")

        in_range_data = data[(date >= oldest_month_begin) & (date < next_month_begin)]
        if in_range_data.empty:
            raise RuntimeError(
                f"No trips between {oldest_month_begin} and {next_month_begin}!"
            )
        return in_range_data

    def run(self):
        """Calculate the rolling averages for the month."""
        df = pd.concat(
            [read_taxi_dataframe(target.path) for target in self.input()],
            ignore_index=True,
        )
        df = add_trip_duration(df)
        df = reject_outliers(
            df, max_duration_s=self.max_duration, max_distance=self.max_distance
        )

        df.set_index("tpep_dropoff_datetime", inplace=True, drop=True)
        df = self._reject_not_in_range(df)

        this_month_begin = pd.Timestamp(self.year, self.month, 1)
        rolling_means_by_trip = rolling_means(
            df, n_window_days=self.window, keep_after=this_month_begin
        )
        # Only keep the last rolling mean of each day.
        # Massively reduces output data size
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
        for date in fetch.available_dataset_dates():
            yield self.clone(
                RollingAveragesTask,
                year=date.year,
                month=date.month,
            )

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
