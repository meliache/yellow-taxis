import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pytest import approx
from yellow_taxis.dataframe_utils import (
    add_trip_duration,
    read_taxi_dataframe,
    reject_outliers,
    rolling_means,
)


class TestReadTaxiDataFrame:
    def test_recent_taxi_data_readable_not_empty(self) -> None:
        df = read_taxi_dataframe(
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2023-01_head_for_testing.parquet"
        )
        assert not df.empty

    def test_recent_taxi_data_expected_columns(self) -> None:
        df = read_taxi_dataframe(
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2023-01_head_for_testing.parquet"
        )
        assert df.columns.to_list() == [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
        ]

    def test_read_taxi_data_converts_columns_from_first_schema(self) -> None:
        fpath = (
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2009-01_head_n10_for_testing.parquet"
        )
        orig_df = pd.read_parquet(fpath)
        old_column_names = [
            "Trip_Pickup_DateTime",
            "Trip_Dropoff_DateTime",
            "Trip_Distance",
        ]
        assert set(orig_df.columns).issuperset(old_column_names)

        df = read_taxi_dataframe(fpath)
        assert df.columns.to_list() == [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
        ]

    def test_read_taxi_data_converts_columns_from_second_schema(self) -> None:
        fpath = (
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2010-12_head_n10_for_testing.parquet"
        )
        orig_df = pd.read_parquet(fpath)
        old_column_names = ["pickup_datetime", "dropoff_datetime", "trip_distance"]
        assert set(orig_df.columns).issuperset(old_column_names)

        df = read_taxi_dataframe(fpath)
        assert df.columns.to_list() == [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
        ]

    def test_read_taxi_data_converts_first_schema_cols_to_datetime(self) -> None:
        """The old parquet files stored dates as strings, check if they are
        converted to Timestamps."""
        fpath = (
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2009-01_head_n10_for_testing.parquet"
        )
        df = read_taxi_dataframe(fpath)
        assert df["tpep_pickup_datetime"].dtype == np.dtype("datetime64[ns]")
        assert df["tpep_dropoff_datetime"].dtype == np.dtype("datetime64[ns]")

    def test_read_taxi_data_converts_second_schema_cols_to_datetime(self) -> None:
        """The old parquet files stored dates as strings, check if they are
        converted to Timestamps."""
        fpath = (
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2010-12_head_n10_for_testing.parquet"
        )
        df = read_taxi_dataframe(fpath)
        assert df["tpep_pickup_datetime"].dtype == np.dtype("datetime64[ns]")
        assert df["tpep_dropoff_datetime"].dtype == np.dtype("datetime64[ns]")

    def test_read_taxi_data_fails_if_unkown_columns(self) -> None:
        df = read_taxi_dataframe(
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2023-01_head_for_testing.parquet"
        )
        df.rename(
            columns={
                "tpep_pickup_datetime": "tpep_pickup_datetime_renamed",
            },
            inplace=True,
        )
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_fname = Path(tmpdirname) / "data.parquet"
            df.to_parquet(tmp_fname)
            with pytest.raises(ValueError):
                read_taxi_dataframe(tmp_fname)


class TestRejectOutliers:
    test_data = pd.DataFrame(
        {
            "trip_duration": [20_000, 14000, 10, -1, 3000, 19000],
            "trip_distance": [1, 999999, -10, 1, 500, 999999],
        }
    )

    def test_reject_outliers_reject_duration(self):
        cleaned = reject_outliers(
            self.test_data,
            max_duration_s=14_000,
            max_distance=None,
            reject_negative=None,
        )
        cleaned_expected = pd.DataFrame(
            {
                "trip_duration": [14000, 10, -1, 3000],
                "trip_distance": [999999, -10, 1, 500],
            }
        )
        assert cleaned.to_numpy() == approx(cleaned_expected.to_numpy())

    def test_reject_outliers_reject_distance(self):
        cleaned = reject_outliers(
            self.test_data,
            max_distance=500,
            max_duration_s=None,
            reject_negative=False,
        )
        cleaned_expected = pd.DataFrame(
            {
                "trip_duration": [20_000, 10, -1, 3000],
                "trip_distance": [1, -10, 1, 500],
            }
        )
        assert cleaned.to_numpy() == approx(cleaned_expected.to_numpy())

    def test_reject_outliers_reject_negative(self):
        cleaned = reject_outliers(
            self.test_data,
            max_distance=None,
            max_duration_s=None,
            reject_negative=True,
        )
        cleaned_expected = pd.DataFrame(
            {
                "trip_duration": [20_000, 14000, 3000, 19000],
                "trip_distance": [1, 999999, 500, 999999],
            }
        )
        assert cleaned.to_numpy() == approx(cleaned_expected.to_numpy())

    def test_reject_outliers_raises_empty(self):
        with pytest.raises(RuntimeError):
            reject_outliers(
                self.test_data,
                max_distance=1,
                max_duration_s=1,
                reject_negative=True,
            )


class TestRollingMeans:
    def test_rolling_means_on_dummy_data(self):
        test_data = pd.DataFrame(
            {
                "2023-01-31 12:00:00": [19, 4],
                "2023-01-10 23:30:00": [6, 18],
                # only rolling average for latest time kept
                "2023-01-09 23:30:00": [4, 2],
                "2023-01-09 00:00:00": [2, 4],
                "2023-01-01 00:00:00": [8, 12],
                # should be taken into account in average, but not appear as date
                "2022-12-25 00:00:00": [3, 5],
                "2022-12-01 00:00:00": [1, 1],
            }
        ).T
        test_data.index = pd.to_datetime(test_data.index)
        test_data.sort_index(inplace=True)
        test_data.columns = ["trip_duration", "trip_distance"]
        this_month_begin = pd.Timestamp(2023, 1, 1)
        rolling_averages = rolling_means(
            test_data, n_window_days=45, keep_after=this_month_begin
        )
        expected = pd.DataFrame(
            data=[
                [4.0, 6.0],
                [3.5, 5.5],
                [3.6, 4.8],
                [4.0, 7.0],
                [7.0, 7.5],
            ],
            columns=test_data.columns,
            index=test_data.index[:-2],  # two dates out of range
        )
        assert rolling_averages.to_numpy() == approx(expected.to_numpy())

    def test_rolling_means_2023_01_head_data(self):
        test_data = pd.read_parquet(
            Path(__file__).parent
            / "test_data"
            / "yellow_tripdata_2023-01_head_for_testing.parquet"
        )
        test_data = add_trip_duration(test_data)
        test_data.set_index("tpep_dropoff_datetime", inplace=True, drop=True)
        expected_rolling_averages = pd.DataFrame(
            data={
                "trip_duration": [
                    577.0,
                    613.5,
                    664.0,
                    624.5,
                    575.4,
                ],
                "trip_distance": [1.900000, 1.665000, 1.946667, 1.702500, 1.582000],
            },
        )
        rolling_averages = rolling_means(test_data, n_window_days=45)
        assert rolling_averages.to_numpy() == approx(
            expected_rolling_averages.to_numpy()
        )
