#!/usr/bin/env python3

from pathlib import Path
from typing import Any

import luigi

from yellow_taxis import fetch
from yellow_taxis.tasks import task_utils


class DownloadTask(luigi.Task):
    result_dir = luigi.PathParameter(absolute=True)

    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def result_path(self) -> Path:
        return (
            task_utils.year_month_result_dir(self.result_dir, self.year, self.month)
            / f"yellow_tripdata_{self.year:d}-{self.month:02d}.parquet"
        )

    resources: dict[str, Any] = {
        "downloads": 1,
        "cpus": 1,
        # memory in MB conservative, estimate,
        # see https://daniel.haxx.se/blog/2021/01/21/more-on-less-curl-memory
        "memory": 1,
    }

    def run(self):
        fetch.download_monthly_data(
            self.year,
            self.month,
            file_name=self.result_path,
            make_directories=True,
            overwrite=False,
        )

    def output(self):
        return luigi.LocalTarget(self.result_path)


class AggregateDownloadsTask(luigi.WrapperTask):
    result_dir = luigi.PathParameter(absolute=True)

    def requires(self):
        for date in fetch.available_dataset_dates():
            yield self.clone(
                DownloadTask,
                year=date.year,
                month=date.month,
            )


# TODO implement some settings system to set result directory etc.
repo_root = (Path(__file__).parent.parent.parent).absolute()
RESULT_DIR = repo_root / "data"


def run_locally() -> None:
    luigi.build(
        [AggregateDownloadsTask(result_dir=RESULT_DIR)], local_scheduler=True, workers=1
    )


if __name__ == "__main__":
    run_locally()
