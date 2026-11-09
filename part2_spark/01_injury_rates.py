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
