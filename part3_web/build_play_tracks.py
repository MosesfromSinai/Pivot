import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

# Import the shared Part 2 Spark and MySQL helper.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "part2_spark"))

from common import (
    get_spark,
    write_table,
    MYSQL_URL,
    MYSQL_USER,
    MYSQL_PASSWORD,
)

spark = get_spark("NFL_Play_Tracks")

BASE = os.path.expanduser("~/nfl_data/clean")

# Read the movement features stored during Part 2.
plays = (
    spark.read.format("jdbc")
    .option("url", MYSQL_URL)
    .option("dbtable", "play_features")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)

# Select injury plays.
injury_plays = (
    plays.filter(F.col("is_injury") == True)
    .select("play_key")
)

# Select a reproducible sample of 500 normal plays.
normal_plays = (
    plays.filter(F.col("is_injury") == False)
    .select("play_key")
    .orderBy(F.rand(seed=42))
    .limit(500)
)

selected_plays = (
    injury_plays.union(normal_plays)
    .distinct()
    .cache()
)

# Read the original tracking data.
tracking = spark.read.parquet(
    os.path.join(BASE, "kaggle_tracking")
)

# Keep tracking records for selected plays only.
tracks = (
    tracking.select(
        F.col("PlayKey").alias("play_key"),
        "time",
        "event",
        "x",
        "y",
        F.col("s").alias("speed"),
        "dir",
    )
    .join(F.broadcast(selected_plays), "play_key", "inner")
)

# Use the same event boundaries as Part 2.
start_events = [
    "ball_snap", "snap_direct", "kickoff", "free_kick"
]

end_events = [
    "tackle", "pass_outcome_incomplete",
    "out_of_bounds", "touchdown",
    "pass_outcome_touchdown", "qb_sack",
    "fair_catch", "touchback", "punt_downed",
    "pass_outcome_interception", "qb_spike", "qb_kneel"
]

starts = (
    tracks.filter(F.col("event").isin(start_events))
    .groupBy("play_key")
    .agg(F.min("time").alias("start_time"))
)

ends = (
    tracks.filter(F.col("event").isin(end_events))
    .select("play_key", F.col("time").alias("end_time"))
)

boundaries = (
    starts.join(ends, "play_key")
    .filter(F.col("end_time") > F.col("start_time"))
    .groupBy("play_key", "start_time")
    .agg(F.min("end_time").alias("end_time"))
)

# Retain only active-play tracking records.
play_tracks = (
    tracks.join(boundaries, "play_key")
    .filter(
        (F.col("time") >= F.col("start_time"))
        & (F.col("time") <= F.col("end_time"))
    )
    .select("play_key", "time", "x", "y", "speed", "dir")
    .dropna(subset=["play_key", "time", "x", "y"])
    .dropDuplicates(["play_key", "time"])
    .cache()
)

# Validate the output.
print("\n===== PLAY TRACKS =====")
print("Selected injury plays:", injury_plays.count())
print("Selected normal plays:", normal_plays.count())
print("Available plays:", play_tracks.select("play_key").distinct().count())
print("Tracking records:", play_tracks.count())

play_tracks.orderBy("play_key", "time").show(10, truncate=False)

# Only publish when explicitly requested.
if "--publish" in sys.argv:
    write_table(play_tracks, "play_tracks")
    print("Play tracks saved to MySQL.")
else:
    print("Read-only test completed.")

play_tracks.unpersist()
selected_plays.unpersist()
spark.stop()