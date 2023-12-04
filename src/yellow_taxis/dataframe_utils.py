"""Utilities for working with NYC taxi data dataframes."""
from os import PathLike

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime

# Required columns. These are from the documented schema, since 2015
COLUMN_NAMES = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]

# Historical data from 2015 and earlier uses different column name conventions.
COLUMN_NAME_VARIATIONS = (
    COLUMN_NAMES,
    ["Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Trip_Distance"],
    ["pickup_datetime", "dropoff_datetime", "trip_distance"],
)


def read_taxi_dataframe(file_name: PathLike) -> pd.DataFrame:
    """Read parquet file with NYC yellow-taxi data into dataframe.

    Normalizes also all data to the same schema type with timestamp-colums of datetime
    type.

    :param file_name: Input file parquet file path.
    :return: Pandas dataframe with (normalized to latest format).
    """
    # handle different historical input schemas
    for columns in COLUMN_NAME_VARIATIONS:
        try:
            df = pd.read_parquet(file_name, columns=columns)
            df.columns = COLUMN_NAMES

            # ensure datetime columns are of date type and not just strings
            time_format = "%Y-%m-%d %H:%M:%S"
            for time_col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                df[time_col] = pd.to_datetime(df[time_col], format=time_format)

            return df

        except ValueError:  # try different column set
            pass

    raise ValueError(
        f"Parquet file contains none of the column sets {COLUMN_NAME_VARIATIONS}!"
    )


def reject_not_in_month(
    data: pd.DataFrame, year: int, month: int, on: str | None = None
) -> pd.DataFrame:
    """Return dataframe with all entries removed that outside of given month and year.

    :param data: Pandas dataframe with trip data.
    :param year: Year in which the trip should be.
    :param month: Month in which the trip should be.
    :param on: Datetime column name based on which it's decided whether the trip is in
        the given month. If not given, use dataframe index.
    :return: Pandas dataframe with trip entries outside given month removed.
    """
    month_start = pd.Timestamp(year, month, 1)
    next_month_start = pd.Timestamp(year, month, 1) + pd.tseries.offsets.MonthBegin(1)

    date = data[on] if on else data.index
    if not is_datetime(date):
        raise ValueError(f"Date should be a datetime but is type {date.dtype}!")

    return data[(date > month_start) & (date < next_month_start)]


def trip_duration_s(data: pd.DataFrame) -> pd.Series:
    """Calculate trip duration in seconds.

    :param data: Pandas dataframe with ``tpep_dropoff_datetime`` and
        ``tpep_pickup_datetime`` columns of type ``pd.Timestamp``
    :return: Pandas series with trip duration seconds.
    """
    durations = data["tpep_dropoff_datetime"] - data["tpep_pickup_datetime"]
    return durations.dt.seconds


def add_trip_duration(data: pd.DataFrame) -> pd.DataFrame:
    """Return data with trip duration in seconds added to ``trip_duration`` column.

    :param data: Pandas dataframe with ``tpep_dropoff_datetime`` and
        ``tpep_pickup_datetime`` columns of type ``pd.Timestamp``
    :return: Pandas dataframe with trip duration seconds in ``trip_duration`` columnm.
    """
    data = data.copy()  # avoid modifying existing dataframe
    data["trip_duration"] = trip_duration_s(data)
    return data


def reject_outliers(
    data: pd.DataFrame,
    max_duration_s: int | None = 14_400,
    max_distance: int | None = 1000,
    reject_negative: bool = True,
) -> pd.DataFrame:
    """Reject trip data with unreasonably high or negative trip lengths.

    :param data: Pandas dataframe with ``trip_duration`` and ``trip_distance``
        columns of type float.
    :param max_duration_s: Maximum trip duration in seconds to keep.
        By default 14400 which corresponds to 4h.
    :param max_distance: Maximum trip distance to keep.
    :param reject_negative: Reject trip with negative distances or durations.
    :return: Dataframe with rejected trips removed.
    """
    if max_duration_s:
        data = data[data["trip_duration"] <= max_duration_s]

    if max_distance:
        data = data[data["trip_distance"] <= max_distance]

    if reject_negative:
        data = data[(data["trip_distance"] >= 0) & (data["trip_duration"] >= 0)]

    return data


def rolling_means(
    data: pd.DataFrame,
    n_window_days: int,
    keep_after: pd.Timestamp | None = None,
    on: str | None = None,
    trip_length_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Calculate rolling means of trip lengths for taxi data.

    :param data: Pandas dataframe with trip lengths data.
    :param n_window_days: Number of days to use in rolling means calculation.
    :param keep_after: If given, only keep rolling means with the datetime after this.
        The time refers to the time at the end of the window. Useful when including time
        entries in the rolling mean calculation but excluding them from the result.
    :param on: Column name with the timestamps to use for the rolling means calculation.
        If not given, use index.
    :param trip_length_columns: List of data columns to calculate the rolling means for.
        If ``None``, use the default of ``["trip_duration", "trip_distance"]``.
    :return: Dataframe of rolling means results.
    """
    if on:
        data = data.set_index(on)
    data = data.sort_index()

    if trip_length_columns is None:
        trip_length_columns = ["trip_duration", "trip_distance"]

    trip_lenghts = data[trip_length_columns]
    rolling = trip_lenghts.rolling(pd.Timedelta(days=n_window_days))
    rolling_means = rolling.mean()

    if keep_after:
        return rolling_means[rolling_means.index > keep_after]
    return rolling_means
