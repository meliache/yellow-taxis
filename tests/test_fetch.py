import json
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from subprocess import CalledProcessError

import pandas as pd
import pytest
import validators
from yellow_taxis import fetch


class TestDatasetURLs:
    """Tests for generation of the the dataset URLs."""

    def test_dataset_url_formatting_2023_9_as_expected(self) -> None:
        year: int = 2023
        month: int = 9
        expected: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet"
        assert fetch.dataset_url(year=year, month=month) == expected
        assert fetch.dataset_url(year, month) == expected

    def test_dataset_url_gives_valid_url(self) -> None:
        url: str = fetch.dataset_url(2023, 9)
        assert validators.url(url) is True

    def test_dataset_url_wrong_arg_order(self) -> None:
        with pytest.raises(ValueError):
            fetch.dataset_url(9, 2023)

    def test_dataset_url_month_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            fetch.dataset_url(2023, 0)

        with pytest.raises(ValueError):
            fetch.dataset_url(2023, 13)

        with pytest.raises(ValueError):
            fetch.dataset_url(2023, -1)

    def test_float_dates_raise(self) -> None:
        with pytest.raises(TypeError):
            fetch.dataset_url(2023.0, 9)

        with pytest.raises(TypeError):
            fetch.dataset_url(2023, 9.0)

    def test_dataset_url_year_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            fetch.dataset_url(1992, 9)

    def datasets_for_past_months_exist(self) -> None:
        for year, month in ((2009, 1), (2023, 9), (2018, 8)):
            assert fetch.dataset_exists(year, month)
            time.sleep(1)  # avoid DDOS protection

    def datasets_for_future_does_not_exist(self) -> None:
        future: datetime = datetime.today() + timedelta(days=42)
        assert not fetch.dataset_exists(future.year, future.month)

    def test_generate_available_datasets(self) -> None:
        with open(
            Path(__file__).parent / "test_data" / "available_datasets_2023-11-29.json",
        ) as f:
            available_until_sep_23 = list(sorted(json.load(f)))
            available_now = list(sorted(fetch.available_dataset_urls()))
            assert (
                available_now[: len(available_until_sep_23)] == available_until_sep_23
            )


class TestDownload:
    """Test whether the download works."""

    # URL contains copy of data file with just 5 rows to allow for quick download tests
    test_parquet_url = (
        "https://github.com/meliache/yellow-taxis/raw/main/tests/"
        "test_data/yellow_tripdata_2023-01_head_for_testing.parquet"
    )

    def test_download_valid_url_parquet_reabable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            fname = Path(tmpdirname) / "test.parquet"
            fetch.download(self.test_parquet_url, fname, make_directories=True)

            df: pd.DataFrame = pd.read_parquet(fname)
            # some checks dataframe is not empty
            assert not df.empty and len(df.shape) == 2
            assert df.shape[0] > 0, df.shape[1] > 0

    def test_download_valid_url_make_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            fname = (
                Path(tmpdirname) / "2023" / "01" / self.test_parquet_url.split("/")[-1]
            )
            fetch.download(
                self.test_parquet_url, fname
            )  #  make_directories=True is the default

    def test_download_valid_url_no_make_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            fname = Path(tmpdirname) / self.test_parquet_url.split("/")[-1]
            fetch.download(self.test_parquet_url, fname, make_directories=False)

    def test_download_valid_url_no_make_directories_nonexistent_destdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            fname = (
                Path(tmpdirname) / "2023" / "01" / self.test_parquet_url.split("/")[-1]
            )
            with pytest.raises(CalledProcessError):
                fetch.download(self.test_parquet_url, fname, make_directories=False)
