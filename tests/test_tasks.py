import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
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
    def test_months_required_is_three(self) -> None:
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

    def test_months_required_in_firt_month_of_records(self) -> None:
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
