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

print("Selected injury plays:", injury_plays.count())
print("Selected normal plays:", normal_plays.count())

selected_plays.unpersist()
spark.stop()