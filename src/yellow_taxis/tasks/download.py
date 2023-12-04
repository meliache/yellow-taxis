#!/usr/bin/env python3

from pathlib import Path
from typing import Any

import luigi

from yellow_taxis import fetch
from yellow_taxis.task_utils import TaxiBaseTask


class DownloadTask(TaxiBaseTask):
    """Task to download the parquet file for a given month."""

    year = luigi.IntParameter(description="Dataset year")
    month = luigi.IntParameter(description="Dataset month")

    output_base_name = Path("yellow_tripdata.parquet")

    # Define resource usage of task so that total resource usage can be kept under

    resources: dict[str, Any] = {
        "downloads": 1,
        "cpus": 1,
        # memory in MB conservative, estimate,
        # see https://daniel.haxx.se/blog/2021/01/21/more-on-less-curl-memory
        "memory": 1,
    }

    def run(self):
        """Download dataset."""

        fetch.download_monthly_data(
            self.year,
            self.month,
            file_name=self.get_output_path(),  # download function is already atomic
            make_directories=True,
            overwrite=False,
        )


class DownloadTasksWrapper(luigi.WrapperTask):
    """Wrapper task for running all download tasks."""

    def requires(self):
        """Require the downloads for all months with NYC yellow taxi data."""
        for date in fetch.available_dataset_dates():
            yield self.clone(
                DownloadTask,
                year=date.year,
                month=date.month,
            )


def run_locally() -> None:
    """Run pipeline for downloads locally."""
    luigi.build([DownloadTasksWrapper()], local_scheduler=True, workers=1)


if __name__ == "__main__":
    run_locally()
