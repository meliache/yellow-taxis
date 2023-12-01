"""Utilities for working with NYC taxi data dataframes."""
from os import PathLike

import pandas as pd

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


def reject_not_in_month(data: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Return dataframe with all entries removed that outside of given month and year.

    Whether the entry is in the month will be determined by the index which must be a
    datetime index.
    """
    if not isinstance(data.index.dtype, pd.DatetimeIndex):
        raise RuntimeError("Provide a dataframe with a datetime index!")

    month_start = pd.Timestamp(year, month, 1)
    next_month_start = pd.Timestamp(year, month, 1) + pd.tseries.offsets.MonthBegin(1)
    return data[(data.index > month_start) & (data.index < next_month_start)]
