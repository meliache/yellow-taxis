import pytest
from yellow_taxis import fetch


class TestFetchURLs:
    """Tests for generating the dataset URLs and checking that they exist"""

    def test_dataset_url_formatting_2023_9_nominal(self) -> None:
        year: int = 2023
        month: int = 9
        expected: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet"
        assert fetch.dataset_url(year=year, month=month) == expected
        assert fetch.dataset_url(year, month) == expected

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
