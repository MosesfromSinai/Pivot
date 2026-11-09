import os

from pyspark.sql import functions as F

from common import get_spark, write_table

base = os.path.expanduser("~/nfl_data/clean")

spark = get_spark("NFL_Injury_Rates")

plays = spark.read.parquet(os.path.join(base, "kaggle_plays"))
injuries = spark.read.parquet(os.path.join(base, "kaggle_injuries"))

# Surface, stadium, weather, temperature, and position don't change within
# a player's game, so one row per player-game is enough to label injuries
player_games = plays.groupBy("PlayerKey", "GameID").agg(
    F.first("FieldType").alias("FieldType"),
    F.first("RosterPosition").alias("RosterPosition"),
    F.first("StadiumCategory").alias("StadiumCategory"),
    F.first("WeatherCategory").alias("WeatherCategory"),
    F.first("TemperatureBand").alias("TemperatureBand"),
)

# Join on (PlayerKey, GameID) so injuries without a PlayKey still count
injured = injuries.join(player_games, ["PlayerKey", "GameID"], "left").withColumn(
    "Severity",
    F.when(F.col("DM_M28").cast("int") == 1, "28+ days").otherwise("under 28 days"),
)

# Play-level groups: the denominator is the plays in each group
play_groups = {
    "overall": F.lit("all"),
    "roster_position": F.col("RosterPosition"),
    "stadium": F.col("StadiumCategory"),
    "weather": F.col("WeatherCategory"),
    "temperature_band": F.col("TemperatureBand"),
}

# Injury-level groups: plays have no body part or severity, so the
# denominator is every play on that surface
injury_groups = {"body_part": F.col("BodyPart"), "severity": F.col("Severity")}
surface_plays = plays.groupBy("FieldType").agg(F.count("*").alias("plays"))


def rates(group_type, value, denominators):
    counts = injured.groupBy(value.alias("group_value"), "FieldType").agg(
        F.count("*").alias("injuries")
    )
    return denominators.join(counts, ["group_value", "FieldType"], "left").select(
        F.lit(group_type).alias("group_type"),
        "group_value",
        F.col("FieldType").alias("field_type"),
        "plays",
        F.coalesce("injuries", F.lit(0)).alias("injuries"),
        F.round(F.coalesce("injuries", F.lit(0)) / F.col("plays") * 1000, 3).alias(
            "injuries_per_1000"
        ),
    )


frames = []
for group_type, value in play_groups.items():
    grouped = plays.groupBy(value.alias("group_value"), "FieldType")
    frames.append(rates(group_type, value, grouped.agg(F.count("*").alias("plays"))))
for group_type, value in injury_groups.items():
    values = injured.select(value.alias("group_value")).distinct()
    frames.append(rates(group_type, value, values.crossJoin(surface_plays)))

injury_rates = frames[0]
for frame in frames[1:]:
    injury_rates = injury_rates.unionByName(frame)
