#!/usr/bin/env python3

from pathlib import Path

import luigi

from yellow_taxis import fetch


class DownloadTask(luigi.Task):
    result_dir = luigi.PathParameter(absolute=True)

    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def result_path(self) -> Path:
        return (
            Path(self.result_dir)
            / f"{self.year:d}"
            / f"{self.month:02d}"
            / f"yellow_tripdata_{self.year:d}-{self.month:02d}.parquet"
        )

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


class MainTask(luigi.WrapperTask):
    result_dir = luigi.PathParameter(absolute=True)

    def requires(self):
        for date in fetch.available_dataset_dates():
            yield self.clone(
                DownloadTask,
                year=date.year,
                month=date.month,
            )


if __name__ == "__main__":
    # TODO implement some settings system to set result directory etc.
    repo_root = (Path(__file__).parent.parent.parent).absolute()
    result_dir = repo_root / "data"
    luigi.build([MainTask(result_dir=result_dir)], local_scheduler=True)
