"""Utilities for working with NYC taxi data dataframes."""
import datetime
import logging
from os import PathLike

import polars as pl
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

# Required columns. These are from the documented schema, since 2015
COLUMN_NAMES = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]

# Historical data from 2015 and earlier uses different column name conventions.
COLUMN_NAME_VARIATIONS = (
    COLUMN_NAMES,
    ["Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Trip_Distance"],
    ["pickup_datetime", "dropoff_datetime", "trip_distance"],
)


def read_taxi_dataframe(file_name: PathLike) -> pl.DataFrame:
    """Read parquet file with NYC yellow-taxi data into dataframe.

    Normalizes also all data to the same schema type with timestamp-
    colums of datetime type.

    :param file_name: Input file parquet file path.
    :return: Polars dataframe with (normalized to latest format).
    """
    # handle different historical input schemas
    for columns in COLUMN_NAME_VARIATIONS:
        try:
            df = pl.read_parquet(file_name, columns=columns)
            # normalize columns names to default schema
            df.columns = COLUMN_NAMES
            return time_columns_to_datetime(df)
        except pl.ColumnNotFoundError:
            logger.info(
                "Could not read dataframe with columns %s, trying next column set.",
                columns,
            )
    raise pl.ColumnNotFoundError(
        f"Could not read data from {file_name} "
        "with any column set in {COLUMN_NAME_VARIATIONS}!"
    )


def time_columns_to_datetime(data: pl.DataFrame) -> pl.DataFrame:
    """Convert all time columns in dataframe to ``datetime64[ns]`` format.

    Older data has times in string-format. Some newer data frames also have
    different resolution datetime formats like ``datetime64[us]``,
    which can lead to bugs such as https://github.com/pandas-dev/pandas/issues/55067.
    """

    time_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # ensure datetime columns are of date type and not just strings
    time_format = "%Y-%m-%d %H:%M:%S"
    for time_col in time_columns:
        # convert to datetime
        if not isinstance(data[time_col], pl.Datetime):
            data = data.with_columns(pl.col(time_col).str.to_datetime(time_format))
    return data


def reject_not_in_month(
    data: pl.DataFrame, month_date: datetime.date, on: str
) -> pl.DataFrame:
    """Return dataframe with all entries removed that outside of given month
    and year.

    Needed because some yellow taxi data files for a given month have
    trip data with non-sense dates, probably due to errors during data
    entry.

    :param month_date: Datetime for year and month of dataset (beginning
        of month)
    :param month: Month in which the trip should be.
    :param on: Datetime column name based on which it's decided whether
        the trip is in the given month.
    :return: Polars dataframe with trip entries outside given month
        removed.
    """
    if not month_date == month_date.replace(day=1):
        raise ValueError(f"Date {month_date=} should be beginning of month!")
    month_date = month_date
    next_month_start = month_date + relativedelta(months=1)

    date = data[on]
    if not isinstance(date.dtype, pl.Datetime):
        raise ValueError(f"Date should be a datetime but is type {date.dtype}!")

    data_in_month = data[(date > month_date) & (date < next_month_start)]

    if len(data) == 0:
        raise RuntimeError("Data contains no trips in given month!")
    return data_in_month


def trip_duration_s(data: pl.DataFrame) -> pl.Series:
    """Calculate trip duration in seconds.

    :param data: Polars dataframe with ``tpep_dropoff_datetime`` and
        ``tpep_pickup_datetime`` columns of datetime type.
    :return: Polars series with trip duration seconds.
    """
    durations = data["tpep_dropoff_datetime"] - data["tpep_pickup_datetime"]
    return durations.dt.seconds()


def add_trip_duration(data: pl.DataFrame) -> pl.DataFrame:
    """Return data with trip duration in seconds added to ``trip_duration``
    column.

    :param data: Polars dataframe with ``tpep_dropoff_datetime`` and
        ``tpep_pickup_datetime`` columns of datetime type.
    :return: Polars dataframe with trip duration seconds in ``trip_duration`` columnm.
    """
    durations = trip_duration_s(data)
    return data.with_columns(durations.alias("trip_duration"))


def reject_outliers(
    data: pl.DataFrame,
    max_duration_s: int | None,
    max_distance: int | None,
    reject_negative: bool = True,
) -> pl.DataFrame:
    """Reject trip data with unreasonably high or negative trip lengths.

    :param data: Polars dataframe with ``trip_duration`` and ``trip_distance``
        columns of type float.
    :param max_duration_s: Maximum trip duration in seconds to keep.
        By default 14400 which corresponds to 4h.
    :param max_distance: Maximum trip distance to keep.
    :param reject_negative: Reject trip with negative distances or durations.
    :return: Dataframe with rejected trips removed.
    """
    if max_duration_s:
        data = data.filter(data["trip_duration"] <= max_duration_s)

    if max_distance:
        data = data.filter(data["trip_distance"] <= max_distance)

    if reject_negative:
        data = data.filter((data["trip_distance"] >= 0) & (data["trip_duration"] >= 0))

    if len(data) == 0:
        raise RuntimeError("No trips remain after outlier detection!")

    return data


def rolling_means(
    data: pl.DataFrame,
    n_window_days: int,
    on: str,
    keep_after: datetime.date | None = None,
    trip_length_columns: list[str] | None = None,
) -> pl.DataFrame:
    """Calculate rolling means of trip lengths for taxi data.

    :param data: Polars dataframe with trip lengths data.
    :param n_window_days: Number of days to use in rolling means calculation.
    :param on: Column name with the timestamps to use for the rolling means calculation.
    :param keep_after: If given, only keep rolling means with the datetime after this.
        The time refers to the time at the end of the window. Useful when including time
        entries in the rolling mean calculation but excluding them from the result.
    :param trip_length_columns: List of data columns to calculate the rolling means for.
        If ``None``, use the default of ``["trip_duration", "trip_distance"]``.
    :return: Dataframe of rolling means results.
    """
    data = data.sort(on)

    if trip_length_columns is None:
        trip_length_columns = ["trip_duration", "trip_distance"]

    rolling_means = data[trip_length_columns + [on]]
    for col in trip_length_columns:
        rolling_means = rolling_means.with_columns(
            pl.col(col)
            .rolling_mean(window_size=datetime.timedelta(days=n_window_days), by=on)
            .alias(col)
        )

    if keep_after:
        rolling_means = rolling_means.filter(rolling_means[on] >= keep_after)
        if len(rolling_means) == 0:
            raise RuntimeError(f"No rolling means after {keep_after}!")

    return rolling_means
