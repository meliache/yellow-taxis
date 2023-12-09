import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import luigi
import pandas as pd
from dateutil.relativedelta import relativedelta
from pytest import approx
from yellow_taxis import fetch
from yellow_taxis.tasks.download import DownloadTask
from yellow_taxis.tasks.monthly_averages import (
    AggregateMonthlyAveragesTask,
    MonthlyAveragesTask,
)
from yellow_taxis.tasks.rolling_averages import (
    AggregateRollingAveragesTask,
    RollingAveragesTask,
)

test_data_fpath_2023_01 = (
    Path(__file__).parent
    / "test_data"
    / "yellow_tripdata_2023-01_head_for_testing.parquet"
)


class TestDownloadTask:
    """Test download task.

    Download methods are already tested in ``test_fetch.py``,
    so here we don't test that the downloads themselves work,
    but that the download functions are called with the correct arguments.
    """

    @patch("yellow_taxis.fetch.download_monthly_data")
    def test_download_task_with_expected_args(
        self, mock_download_monthly_data: Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            download_task = DownloadTask(
                year=2009,
                month=9,
                result_dir=tmpdirname,
            )
            expected_result_path = (
                Path(tmpdirname)
                / "year=2009"
                / "month=9"
                / download_task.output_base_name
            )
            download_task.run()
            mock_download_monthly_data.assert_called_once_with(
                2009,
                9,
                file_name=expected_result_path,
                make_directories=True,
                overwrite=False,
            )


class TestMonthlyAveragesTask:
    @patch("yellow_taxis.tasks.monthly_averages.MonthlyAveragesTask.input")
    def get_run_results_for_2023_01_testdata(self, mock_input) -> pd.DataFrame:
        """Get the results of the ``run`` method on for a single month test
        data.

        (The test data is the head of the original data with just 5
        rows)
        """

        mock_input.return_value = luigi.LocalTarget(test_data_fpath_2023_01)

        with tempfile.TemporaryDirectory() as tmpdirname:
            monthly_avg_task = MonthlyAveragesTask(
                year=2023,
                month=1,
                result_dir=tmpdirname,
            )

            monthly_avg_task.run()
            return pd.read_parquet(monthly_avg_task.get_output_path())

    def test_run_for_2023_01_testdata_duration_mean(self) -> None:
        run_results = self.get_run_results_for_2023_01_testdata()
        assert run_results.loc["trip_duration_mean"].iloc[0] == approx(575.4)

    def test_run_for_2023_01_testdata_duration_sem(self) -> None:
        run_results = self.get_run_results_for_2023_01_testdata()
        assert run_results.loc["trip_duration_mean_err"].iloc[0] == approx(65.15566)

    def test_run_for_2023_01_testdata_distance_mean(self) -> None:
        run_results = self.get_run_results_for_2023_01_testdata()
        assert run_results.loc["trip_distance_mean"].iloc[0] == approx(1.582)

    def test_run_for_2023_01_testdata_distance_sem(self) -> None:
        run_results = self.get_run_results_for_2023_01_testdata()
        assert run_results.loc["trip_distance_mean_err"].iloc[0] == approx(
            0.2821595293446599
        )


class TestRollingAverageTask:
    def test_n_months_required_is_three(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2023,
            month=1,
            window=45,
        )
        assert rolling_avg_task.n_months_required == 3

    def test_n_months_required_in_first_month_of_records(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2009,  # date of first records, meaning no data in the past
            month=1,
            window=45,
        )
        assert rolling_avg_task.n_months_required == 1

    def test_n_months_required_in_second_month_of_records(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2009,  # date of first records, meaning no data in the past
            month=2,
            window=45,
        )
        assert rolling_avg_task.n_months_required == 2

    def test_n_months_required_is_two_for_short_window(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2023,
            month=1,
            window=20,
        )
        assert rolling_avg_task.n_months_required == 2

    def test_months_required_last_three(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2023,
            month=1,
            window=45,
        )
        assert rolling_avg_task._months_required() == [
            pd.Timestamp(2023, 1, 1),
            pd.Timestamp(2022, 12, 1),
            pd.Timestamp(2022, 11, 1),
        ]

    def test_months_required_in_first_month_of_records(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2009,  # date of first records, meaning no data in the past
            month=1,
            window=45,
        )
        assert rolling_avg_task._months_required() == [pd.Timestamp(2009, 1, 1)]

    def test_months_required_in_second_month_of_records(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2009,  # date of first records, meaning no data in the past
            month=2,
            window=45,
        )
        assert rolling_avg_task._months_required() == [
            pd.Timestamp(2009, 2, 1),
            pd.Timestamp(2009, 1, 1),
        ]

    def test_months_required_is_two_for_short_window(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2023,
            month=1,
            window=20,
        )
        assert rolling_avg_task._months_required() == [
            pd.Timestamp(2023, 1, 1),
            pd.Timestamp(2022, 12, 1),
        ]

    def test_reject_not_in_range_on_index(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2023,
            month=1,
            window=45,
        )
        index = pd.DatetimeIndex(
            [
                "2023-01-01",
                "2023-01-29",
                "2023-02-01",  # beginning of next month out-of-range
                "2024-01-01",
                "2022-10-19",
                "2022-11-01",  # beginning of prev-prev month in range
                "2022-12-15",
            ]
        )
        index_in_range = pd.DatetimeIndex(
            [
                "2023-01-01",
                "2023-01-29",
                "2022-11-01",
                "2022-12-15",
            ]
        )
        df = pd.DataFrame(data=[pd.NA] * len(index), index=index)

        index_not_rejected = rolling_avg_task._reject_not_in_range(df).index
        assert index_in_range.equals(index_not_rejected)

    def test_reject_not_in_range_on_col(self) -> None:
        rolling_avg_task = RollingAveragesTask(
            year=2009,
            month=1,
            window=45,
        )
        dates = pd.to_datetime(
            [
                "2009-01-01",
                "2008-12-31",
                "2009-01-31",
                "2009-02-01",
            ]
        )
        dates_in_range = pd.to_datetime(["2009-01-01", "2009-01-31"])
        df = pd.DataFrame(data={"dates": dates})

        dates_not_rejected = rolling_avg_task._reject_not_in_range(df, on="dates")[
            "dates"
        ]
        assert all(dates_in_range.to_numpy() == dates_not_rejected.to_numpy())


class TestAggregateRollingAveragesTask:
    def test_requires(self):
        task = AggregateRollingAveragesTask()
        dates = pd.DatetimeIndex(
            [pd.Timestamp(dep.year, dep.month, 1) for dep in task.requires()]
        )
        dates_expected = pd.DatetimeIndex(fetch.available_dataset_dates())
        assert dates.sort_values().equals(dates_expected.sort_values())

    def test_task_reruns_when_new_data_available(self):
        with tempfile.TemporaryDirectory() as tempdirname:
            task = AggregateRollingAveragesTask(
                result_dir=tempdirname, last_month=fetch.most_recent_dataset_date()
            )
            # first pretend we have generated output for previous months
            Path(task.output().path).touch()
            assert task.complete()
            # now pretend another input dataset suddenly became published
            task = AggregateRollingAveragesTask(
                result_dir=tempdirname,
                last_month=fetch.most_recent_dataset_date() + relativedelta(months=1),
            )
            # now task should NOT be complete
            assert not task.complete()


class TestAggregateMonthlyAveragesTask:
    def test_task_reruns_when_new_data_available(self):
        with tempfile.TemporaryDirectory() as tempdirname:
            task = AggregateMonthlyAveragesTask(
                result_dir=tempdirname, last_month=fetch.most_recent_dataset_date()
            )
            # first pretend we have generated output for previous months
            Path(task.output().path).touch()
            assert task.complete()
            # now pretend another input dataset suddenly became published
            task = AggregateMonthlyAveragesTask(
                result_dir=tempdirname,
                last_month=fetch.most_recent_dataset_date() + relativedelta(months=1),
            )
            # now task should NOT be complete
            assert not task.complete()
