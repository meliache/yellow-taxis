import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from yellow_taxis import fetch
from yellow_taxis.tasks.download import DownloadTask
from yellow_taxis.tasks.rolling_averages import RollingAveragesTask


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
        differences_with_expectation = index_in_range - index_not_rejected
        assert not any(differences_with_expectation)

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
        dates_in_range = pd.DatetimeIndex(
            [
                "2009-01-01",
                "2009-01-31",
            ]
        )
        df = pd.DataFrame(data={"dates": dates})

        dates_not_rejected = rolling_avg_task._reject_not_in_range(df, on="dates")[
            "dates"
        ]
        differences_with_expectation = dates_in_range - dates_not_rejected
        assert not any(differences_with_expectation)


class AggregateRollingAveragesTask:
    def test_requires(self):
        task = AggregateRollingAveragesTask()
        dates = pd.DatetimeIndex(
            [pd.Timestamp(dep.year, dep.month, 1) for dep in task.requires()]
        )
        dates_expected = pd.DatetimeIndex(fetch.available_dataset_dates())
        assert not any(dates.sort_values() - dates_expected.sort_values())

    # TODO test creation of aggregated sampled dataframe
