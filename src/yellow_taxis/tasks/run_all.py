#!/usr/bin/env python3
import luigi

from yellow_taxis.tasks.monthly_averages import AggregateMonthlyAveragesTask
from yellow_taxis.tasks.rolling_averages import AggregateRollingAveragesTask


class MainTask(luigi.WrapperTask):
    def requires(self):
        yield AggregateMonthlyAveragesTask()
        yield AggregateRollingAveragesTask()


def run_locally() -> None:
    """Run pipeline for all tasks."""
    luigi.build(
        [MainTask()],
        local_scheduler=True,
        workers=1,
    )


if __name__ == "__main__":
    run_locally()
