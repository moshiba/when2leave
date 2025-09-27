"""

https://www.cityofmadison.com/metro/business/information-for-developers
"""

from google.transit import gtfs_realtime_pb2
import requests
import click
import polars as pl

import pathlib
import dataclasses
import datetime

# pull daily schedule: https://transitdata.cityofmadison.com/GTFS/mmt_gtfs.zip
# cache with LastUpdated

# keep polling GTFS-RT {trip-updates, alerts, vehicle-position}
"""
GTFS files:
- stops: stations
- routes: group of trips displayed to riders as a single service
- trips: a sequence of two or more stops during a specific time period
- stop_times: times a vehicle arrives at and departs from stops for each trip
- calendar: weekly schedule
- calendar_dates: exceptions for dates defined in calendar.txt
- shapes: rules for mapping vehicle travel paths, aka route alignments
"""


@dataclasses.dataclass
class Stop:
    name: str
    route_id: str
    #stop_id: int


class StopsOfInterest:
    stops = [
        ## HOME
        Stop("Orchard", "AF"),
        Stop("Capitol Square", "AF"),
        Stop("Paterson", "A"),
        Stop("Baldwin", "A"),  # extra
        # C
        Stop("W Johnson / N Orchard", "C"),
        Stop("E Wilson / S Butler", "C38"),
        Stop("Williamson / S Blount", "C38"),  # extra
        Stop("Jenifer / S Livingston", "C38"),  # extra
        Stop("Jenifer / S Paterson", "C38"),
        # 38
        Stop("N Charter / University", "38"),
        # R
        Stop("W Johnson / N Orchard", "R"),
        Stop("W Main / S Carroll", "R"),
    ]


"""
# Real time updates
feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get("https://metromap.cityofmadison.com/gtfsrt/trips")
feed.ParseFromString(response.content)
print(feed.entity[0])
exit()
for entity in feed.entity:
    if entity.HasField("trip_update"):
        print(entity.trip_update)
"""

routes_of_interest = ("A", "C", "R", "F", "38")

q_trips = (
    pl.scan_csv("mmt_gtfs/trips.txt", schema_overrides={"route_id": str})  #
    .filter(pl.col("route_id").is_in(routes_of_interest))  #
    .filter(pl.col("direction_id") == 1)  # eastbound
)

#print(q_trips.profile())
#print(q_trips.explain())
df_trips = q_trips.collect()

# Sanity check
assert set(df_trips.lazy().select("route_id").unique("route_id").collect(
).to_series().to_list()) == set(routes_of_interest), "Some routes got droppped"

print("@Routes of Interest")
print(df_trips.group_by("route_id").len().rename({"len": "#trips"}))
print()
print("@Trips of these routes")
print(df_trips)
print()

uniq_trip_id = df_trips.lazy().select(
    "trip_id").unique().collect().to_series().implode()

time_of_interest = (datetime.time.fromisoformat("15:00"),
                    datetime.time.fromisoformat("18:00"))
q_stop_times = (
    pl.scan_csv("mmt_gtfs/stop_times.txt",
                schema_overrides={
                    "arrival_time": str,
                    "departure_time": str,
                })  #
    .filter(pl.col("trip_id").is_in(uniq_trip_id))  #
    .with_columns(
        pl.col("arrival_time").str.to_time("%H:%M:%S", strict=False),
        pl.col("departure_time").str.to_time("%H:%M:%S", strict=False),
    )  # cleanup illegal times like "24:20:00", "24:21:33", ... "24:29:41"
    .drop_nulls()  # catching 420
    .filter((time_of_interest[0] <= pl.col("arrival_time")) &
            (pl.col("arrival_time") <= time_of_interest[1]) &
            (time_of_interest[0] <= pl.col("departure_time")) &
            (pl.col("departure_time") <= time_of_interest[1]))  #
)

#print(q_stop_times.profile())
df_stop_times = q_stop_times.collect()

print("@Stop times")
print(df_stop_times)
print()

# Locations of interest
# longitude/latitude difference for 500 meters: 0.0045 degrees
# MH door: 43.07285° N, 89.40726° W
loc_oi = {"mh": (43.07285, -89.40726)}  # add OP?
allowed_distance = 0.0045

uniq_stop_id = df_stop_times.lazy().select(
    "stop_id").unique().collect().to_series().implode()

q_stops = (
    pl.scan_csv("mmt_gtfs/stops.txt")  #
    .filter(pl.col("stop_id").is_in(uniq_stop_id))  # my routes + eastbound
)

# Concatenate multiple .when().then()
geo_filter_expr = pl
for name, coordinates in loc_oi.items():
    geo_filter_expr = (
        geo_filter_expr.when(
            (pl.col("stop_lat") >= (coordinates[0] - allowed_distance)) &
            (pl.col("stop_lat") <= (coordinates[0] + allowed_distance)) &
            (pl.col("stop_lon") >= (coordinates[1] - allowed_distance)) &
            (pl.col("stop_lon") <= (coordinates[1] + allowed_distance)))  #
        .then(pl.lit(name))  #
    )

q_stops = (
    q_stops.with_columns(geo_filter_expr.alias("stop_of_interest"))  #
    .filter(pl.col("stop_of_interest").is_in(loc_oi.keys()))  #
)

#print(q_stops.profile())

pl.Config(tbl_rows=-1)
#pl.Config(tbl_cols=-1)

df_stops = q_stops.collect()

print("@Stops of interest")
print(df_stops)
print()

# Stations of interest
