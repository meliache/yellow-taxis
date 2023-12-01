# Notes on "susprises" in the dataset


Analyzing the data was a lot of *fun*, as there were many surprises which take special handling. I was mostly busy building the timeline, not on exploring the data, so I'm sure I missed many caveats, except when they caused my pipeline to break. Here is what I found

## Different column name conventions

I found the following column name conventions (only considerung the columns that I use):

``` python
["Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Trip_Distance"],
["pickup_datetime", "dropoff_datetime", "trip_distance"]
["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]
```

The last one is what's in the official [schema documentation](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) and seems what's used since 2015.

## Different time encodings

In the latest datasets the timestamps are all of a datetime datatype, but for the older datasets they are string timestamps. This is also a reason why the older files are much larger in file size, which threw of my total size estimation.


## Timestamps outside of month range

The parquet file for December 2022 has an entry with pickup-time 2002, though that's likely I typo, no data was recorded then. Some are supposedly from 2008 and 2009. These should just be rejected as unreliable.

However there are some entries with dates just a single day outside of the date given in the parquet file. This are probably valid. But since we analyze months with seperate tasks, they can's easily be moved into the correct bucket. But those Are only a dozen out of several million trip data entries, so I think just rejecting all of these entries will not affect averages.
