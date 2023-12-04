import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from yellow_taxis.tasks.download import DownloadTask


@patch("yellow_taxis.fetch.download_monthly_data")
def test_download_task_with_expected_args(mock_download_monthly_data: Mock):
    with tempfile.TemporaryDirectory() as tmpdirname:
        download_task = DownloadTask(
            year=2009,
            month=9,
            max_duration=14_000,
            max_distance=1000,
            result_dir=tmpdirname,
        )
        expected_result_path = (
            Path(tmpdirname)
            / "max_duration=14000"
            / "max_distance=1000"
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
