
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

base = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/nfl_data")

raw = os.path.join(base, "raw")
clean = os.path.join(base, "clean")

spark = SparkSession.builder.appName("NFL_Data_Cleaning").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

datasets = {
    "kaggle_injuries": ("kaggle/InjuryRecord.csv", "csv"),
    "kaggle_plays": ("kaggle/PlayList.csv", "csv"),
    "kaggle_tracking": ("kaggle/PlayerTrackData.csv", "csv"),
    "nflverse_injuries": ("nflverse/injuries.parquet", "parquet"),
    "nflverse_schedules": ("nflverse/schedules.parquet", "parquet")
}


# Helper functions for evaluation
def count_nulls(df):
    results = {}

    # Count missing values across all columns in one Spark aggregation
    expressions = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]

    counts = df.agg(*expressions).first().asDict()

    for column, count in counts.items():
        if count and count > 0:
            results[column] = count

    return results


def folder_size(path):
    total = 0

    for folder, dirs, files in os.walk(path):
        for filename in files:
            total += os.path.getsize(os.path.join(folder, filename))

    return total


# Functions to clean the NFL data
def clean_data(df, name):

    # Convert empty CSV strings to null values
    if name.startswith("kaggle"):
        for column in df.columns:
            df = df.withColumn(
                column,
                F.when(
                    F.trim(F.col(column)) == "", None
                ).otherwise(F.trim(F.col(column)))
            )

    # Standardize stadium and weather information
    if name == "kaggle_plays":

        stadium = F.lower(F.coalesce(F.col("StadiumType"), F.lit("")))
        weather = F.lower(F.coalesce(F.col("Weather"), F.lit("")))

        df = df.withColumn(
            "StadiumCategory",
            F.when(stadium.rlike("indoor|dome|closed"), "indoor")
             .when(stadium.rlike("outdoor|open"), "outdoor")
             .otherwise("unknown")
        )

        df = df.withColumn(
            "WeatherCategory",
            F.when(weather.rlike("indoor|controlled"), "indoor")
             .when(weather.rlike("snow|flurr"), "snow")
             .when(weather.rlike("rain|shower|drizzle"), "rain")
             .when(weather.rlike("cloud|overcast"), "cloudy")
             .when(weather.rlike("clear|sun|fair"), "clear")
             .otherwise("other")
        )

    # Standardize temperature and create bands
    if name in ("kaggle_plays", "nflverse_schedules"):

        temperature_col = (
            "Temperature" if name == "kaggle_plays" else "temp"
        )

        df = df.withColumn(
            temperature_col,
            F.col(temperature_col).cast("double")
        )

        df = df.withColumn(
            temperature_col,
            F.when(
                F.col(temperature_col) == -999, None
            ).otherwise(F.col(temperature_col))
        )

        df = df.withColumn(
            "TemperatureBand",
            F.when(F.col(temperature_col).isNull(), "unknown")
             .when(F.col(temperature_col) < 40, "cold")
             .when(F.col(temperature_col) < 70, "mild")
             .otherwise("warm")
        )

    # Remove incomplete player tracking records
    if name == "kaggle_tracking":

        for column in ["time", "x", "y", "dir", "dis", "o", "s"]:
            df = df.withColumn(column, F.col(column).cast("double"))

        df = df.dropna(subset=["PlayKey", "time", "x", "y"])

    return df


# Read, clean, and save each dataset
start_time = time.perf_counter()

try:
    for name, (filename, file_type) in datasets.items():

        print(f"\n===== {name} =====", flush=True)

        input_path = os.path.join(raw, filename)
        output_path = os.path.join(clean, name)

        dataset_start = time.perf_counter()

        if file_type == "csv":
            df = spark.read.csv(
                input_path,
                header=True,
                inferSchema=False
            )
        else:
            df = spark.read.parquet(input_path)

        # Evaluate the original dataset
        rows_before = df.count()
        nulls_before = count_nulls(df)

        # Apply the cleaning rules
        cleaned_df = clean_data(df, name)

        # Avoid overwriting teammates' existing output
        cleaned_df.write.mode("errorifexists").parquet(output_path)

        # Read the saved result for evaluation
        saved_df = spark.read.parquet(output_path)

        rows_after = saved_df.count()
        nulls_after = count_nulls(saved_df)

        # Print results for the Part 1 report
        print("Rows before cleaning:", rows_before)
        print("Rows after cleaning:", rows_after)
        print("Rows removed:", rows_before - rows_after)

        print("Null counts before:", nulls_before)
        print("Null counts after:", nulls_after)

        print("Columns:", len(saved_df.columns))
        print("Stored size (bytes):", folder_size(output_path))
        print(
            "Processing time (seconds):",
            round(time.perf_counter() - dataset_start, 2),
            flush=True
        )

    # Print the final evaluation
    total_time = time.perf_counter() - start_time

    print("\n===== FINAL EVALUATION =====")
    print("Spark clean and store time:", round(total_time, 2), "seconds")
    print("Total cleaned data size:", folder_size(clean), "bytes")

finally:
    spark.stop()

