#!/usr/bin/env python3

from pathlib import Path

import luigi
import pandas as pd
from luigi.util import requires

from yellow_taxis import fetch
from yellow_taxis.dataframe_utils import (
    add_trip_duration,
    read_taxi_dataframe,
    reject_not_in_month,
    reject_outliers,
)
from yellow_taxis.task_utils import TaxiBaseTask
from yellow_taxis.tasks.download import DownloadTask


@requires(DownloadTask)
class MonthlyAveragesTask(TaxiBaseTask):
    """Task for calculating monthly averages of taxi travel times and
    distances."""

    # Will uses this for the name the single column dataframe that this task generates.
    # When concatenating results this will give us a date string index. Would have
    # preferred a datetime object but parquet only allows for string column names.
    # Having this a property is important for accessing this in other tasks.
    month_date_fmt = "%Y-%m"

    output_base_name = Path("monthly_average.parquet")

    resources = {
        "cpus": 1,
        "memory": 4_000,
    }

    def run(self):
        """Download the dataset."""
        input_fpath = Path(self.input().path)

        df = read_taxi_dataframe(input_fpath)
        df = reject_not_in_month(
            df, month_date=self.month_date, on="tpep_dropoff_datetime"
        )
        df = add_trip_duration(df)
        df = reject_outliers(
            df,
            max_duration_s=self.max_duration,
            max_distance=self.max_distance,
        )

        results: dict[str, float] = {}
        for col in ["trip_distance", "trip_duration"]:
            # calculate mean and uncertainty on mean
            results[f"{col}_mean"] = df[col].mean()
            results[f"{col}_mean_err"] = df[col].sem()

        result_series = pd.Series(results)
        col_name = self.month_date.strftime(self.month_date_fmt)
        result_df = result_series.to_frame(col_name)

        with self.output().temporary_path() as self.temp_output_path:
            result_df.to_parquet(self.temp_output_path)


class AggregateMonthlyAveragesTask(TaxiBaseTask):
    """Aggregate all monthly averages in a single dataframe."""

    output_base_name = Path("monthly_averages.parquet")

    # Month parameter for most recent available dataset. This will be encoded in the
    # output and thus force the pipeline to be re-run if a new dataset gets published.
    last_month = luigi.MonthParameter(
        default=fetch.most_recent_dataset_date(),
        description="Most recent month for which a dataset is available",
    )

    resources = {"cpus": 1}

    def requires(self):
        for date in fetch.available_dataset_dates(self.last_month):
            yield self.clone(
                MonthlyAveragesTask,
                month_date=date,
            )

    def run(self):
        monthly_averages: pd.DataFrame = pd.concat(
            [pd.read_parquet(input_target.path) for input_target in self.input()],
            axis=1,
        ).T
        monthly_averages.index = pd.to_datetime(
            monthly_averages.index, format=MonthlyAveragesTask.month_date_fmt
        )

        with self.output().temporary_path() as self.temp_output_path:
            monthly_averages.to_parquet(self.temp_output_path)


def run_locally() -> None:
    """Run pipeline for monthly averages locally."""
    luigi.build(
        [
            AggregateMonthlyAveragesTask(),
        ],
        local_scheduler=True,
        workers=1,
    )


if __name__ == "__main__":
    run_locally()
