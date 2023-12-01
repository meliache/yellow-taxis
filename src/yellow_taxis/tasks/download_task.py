#!/usr/bin/env python3

from pathlib import Path
from typing import Any

import luigi

from yellow_taxis import fetch, task_utils


class DownloadTask(luigi.Task):
    """Task to download the parquet file for a given month."""

    result_dir = luigi.PathParameter(
        description="Root directory under which to store downloaded files.",
        absolute=True,
    )

    year = luigi.IntParameter(description="Dataset year")
    month = luigi.IntParameter(description="Dataset month")

    @property
    def result_path(self) -> Path:
        """File path where downloaded data will be saved to."""
        return (
            task_utils.year_month_result_dir(self.result_dir, self.year, self.month)
            / f"yellow_tripdata_{self.year:d}-{self.month:02d}.parquet"
        )

    # Define resource usage of task so that total resource usage can be kept under
    # what's defined in `luigi.cfg`/`luigi.toml`
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
            file_name=self.result_path,
            make_directories=True,
            overwrite=False,
        )

    def output(self):
        return luigi.LocalTarget(self.result_path)


class DownloadTasksWrapper(luigi.WrapperTask):
    reult_dir = luigi.PathParameter(absolute=True)

    def requires(self):
        for date in fetch.available_dataset_dates():
            yield self.clone(
                DownloadTask,
                year=date.year,
                month=date.month,
            )


# TODO implement some settings system to set result directory etc.
repo_root = (Path(__file__).parent.parent.parent.parent).absolute()
RESULT_DIR = repo_root / "data"


def run_locally() -> None:
    luigi.build(
        [DownloadTasksWrapper(result_dir=RESULT_DIR)], local_scheduler=True, workers=1
    )


if __name__ == "__main__":
    run_locally()
