"""
Module for downloading parquet files with monthly trip data from
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
"""

#: format string that given a year and month and can be formatted to a valid parquet
# download URL.
# TODO Is the hashed subdomain 37ci6vzurychx.cloudfront.net fixed or can
# it change?

URL_FORMAT_STRING: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:d}-{month:02d}.parquet"


def dataset_url(
    year: int,
    month: int,
) -> str:
    """Return download URL for parquet file with yellow taxis monthly trip data.

    :param year: Year in which dataset was recorded.
    :param month: Month in which dataset was recorded, as integer starting from 1
    :return: Download URL for parquet file with yellow taxis monthly trip data
    """
    if not isinstance(year, int):
        raise TypeError(f"Year must be an integer but if {year=}")

    if not isinstance(month, int):
        raise TypeError(f"Month must be an integer but if {month=}")

    if not 1 <= month <= 12:
        raise ValueError(
            f"Invalid month, it should be an integer from 1 to 12 but is {month}."
        )
    if year < 2009:
        raise ValueError(f"Historical data for {year} does not exist.")

    return URL_FORMAT_STRING.format(year=year, month=month)


# TODO: to implement
# def download_monthly_data(
#     year: int
#     month: int,
# ) -> None:
#     requests.do
