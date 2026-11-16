import os
from pyspark.sql import functions as F
from common import get_spark, write_table

base = os.path.expanduser("~/nfl_data/clean")
spark = get_spark("NFL_League_Injury_Rates")

injuries = spark.read.parquet(os.path.join(base, "nflverse_injuries"))
schedules = spark.read.parquet(os.path.join(base, "nflverse_schedules"))

# Natural or Synthetic 
surface_text = F.lower(F.trim(F.coalesce(F.col("surface"), F.lit(""))))
schedules = schedules.withColumn(
    "surface_type",
    F.when(surface_text.contains("grass"), "Natural")
    .when(surface_text.rlike("turf|astro|matrix|sport"), "Synthetic")
    .otherwise("Unknown"),
)

# One row per team per game with the game's surface
home = schedules.select(
    "game_id",
    "season",
    "game_type",
    "week",
    F.col("home_team").alias("team"),
    "surface_type",
)
away = schedules.select(
    "game_id",
    "season",
    "game_type",
    "week",
    F.col("away_team").alias("team"),
    "surface_type",
)
team_games = home.unionByName(away)

# An injury is counted based on the weekly injury report, counted once per week.
weekly_injuries = (
    injuries.filter(F.col("report_primary_injury").isNotNull())
    .groupBy("season", "game_type", "week", "team")
    .agg(F.countDistinct("gsis_id").alias("injuries"))
)

matched = team_games.join(
    weekly_injuries, ["season", "game_type", "week", "team"], "left"
).withColumn("injuries", F.coalesce(F.col("injuries"), F.lit(0)))

matched.createOrReplaceTempView("team_games_injuries")

# Games, injuries, and injuries per game by season, team, and surface
league_injury_rates = spark.sql(
    """
    SELECT
        CAST(season AS SMALLINT)                     AS season,
        team,
        surface_type                                 AS surface,
        CAST(COUNT(*) AS INT)                        AS games,
        CAST(SUM(injuries) AS INT)                   AS injuries,
        ROUND(SUM(injuries) / COUNT(*), 3)           AS injuries_per_game
    FROM team_games_injuries
    WHERE team IS NOT NULL AND season IS NOT NULL
    GROUP BY season, team, surface_type
    """
).cache()

write_table(league_injury_rates, "league_injury_rates")

# Print results for the Part 2 report
total_reported = weekly_injuries.agg(F.sum("injuries")).first()[0]
total_matched = matched.agg(F.sum("injuries")).first()[0]
seasons = league_injury_rates.agg(F.min("season"), F.max("season")).first()

print("\n===== Injuries per game (nflverse) =====")
print("Seasons:", seasons[0], "to", seasons[1])
print("Injuries on reports:", total_reported, "| matched to a game:", total_matched)
print("Rows written to league_injury_rates:", league_injury_rates.count())

print("\n----- By surface -----")
league_injury_rates.createOrReplaceTempView("league_rates")
by_surface = spark.sql(
    """
    SELECT
        surface,
        SUM(games)                           AS games,
        SUM(injuries)                        AS injuries,
        ROUND(SUM(injuries) / SUM(games), 3) AS injuries_per_game
    FROM league_rates
    GROUP BY surface
    ORDER BY surface
    """
)
by_surface.show(truncate=False)

print("----- Sample rows (most recent season) -----")
league_injury_rates.orderBy(
    F.col("season").desc(), "team", "surface"
).show(20, truncate=False)

spark.stop()
