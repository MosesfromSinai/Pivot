"""Joshua's movement calculations, shared by the full job and timing adapter."""

import argparse
import os


def build_features(*, spark, tracking, plays, injuries):
    """Build lazy outputs from supplied frames; no startup, reads, caching or writes."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Keep the play metadata needed for our results
    play_info = (
        plays.select(
            "PlayKey", "PlayerKey", "GameID",
            "FieldType", "RosterPosition"
        )
        .dropDuplicates(["PlayKey"])
    )

    # Mark specifically identified injured plays
    injured_plays = (
        injuries.filter(F.col("PlayKey").isNotNull())
        .select("PlayKey")
        .distinct()
        .withColumn("is_injury", F.lit(True))
    )

    # Compare consecutive observations within each play
    window = Window.partitionBy("PlayKey").orderBy("time")

    # Events that begin an active play
    start_events = [
        "ball_snap",
        "snap_direct",
        "kickoff",
        "free_kick",
    ]

    # Events that can signal the end of a play
    end_events = [
        "tackle",
        "pass_outcome_incomplete",
        "out_of_bounds",
        "touchdown",
        "pass_outcome_touchdown",
        "qb_sack",
        "fair_catch",
        "touchback",
        "punt_downed",
        "pass_outcome_interception",
        "qb_spike",
        "qb_kneel",
    ]

    # Find the beginning of each play
    starts = tracking.filter(
        F.col("event").isin(start_events)
    ).groupBy("PlayKey").agg(
        F.min("time").alias("start_time")
    )

    # Find candidate ending events
    ends = tracking.filter(
        F.col("event").isin(end_events)
    ).select(
        "PlayKey",
        F.col("time").alias("end_time")
    )

    # Find the first valid ending after the start
    boundaries = (
        starts.join(ends, "PlayKey", "left")
        .filter(
            F.col("end_time").isNull()
            | (F.col("end_time") > F.col("start_time"))
        )
        .groupBy("PlayKey", "start_time")
        .agg(F.min("end_time").alias("end_time"))
    )

    # Only use tracking records within valid play boundaries
    tracking = (
        tracking.join(boundaries, "PlayKey", "inner")
        .filter(
            F.col("end_time").isNotNull()
            & (F.col("time") >= F.col("start_time"))
            & (F.col("time") <= F.col("end_time"))
        )
    )

    movement = (
        tracking
        .select("PlayKey", "time", "dis", "s", "dir")
        .withColumn("prev_time", F.lag("time").over(window))
        .withColumn("prev_speed", F.lag("s").over(window))
        .withColumn("prev_dir", F.lag("dir").over(window))
        .withColumn("cut_prev_time", F.lag("time", 5).over(window))
        .withColumn("cut_prev_speed", F.lag("s", 5).over(window))
        .withColumn("cut_prev_dir", F.lag("dir", 5).over(window))
    )

    movement = movement.withColumn(
        "delta_time",
        F.col("time") - F.col("prev_time")
    )

    # Positive acceleration in speed-units per second
    movement = movement.withColumn(
        "acceleration",
        F.when(
            (F.col("delta_time") > 0)
            & F.col("s").isNotNull()
            & F.col("prev_speed").isNotNull(),
            (F.col("s") - F.col("prev_speed"))
            / F.col("delta_time")
        )
    )

    # Compare movement direction over approximately 0.5 seconds.
    raw_angle = F.abs(F.col("dir") - F.col("cut_prev_dir"))
    angle = F.pmod(raw_angle, F.lit(360.0))

    movement = movement.withColumn(
        "angle_change",
        F.least(angle, F.lit(360.0) - angle)
    )

    # Identify windows with a substantial directional change.
    movement = movement.withColumn(
        "cut_candidate",
        F.when(
            (F.abs(F.col("time") - F.col("cut_prev_time") - 0.5) < 0.05)
            & (F.col("angle_change") >= 45)
            & (F.col("s") >= 1.0)
            & (F.col("cut_prev_speed") >= 1.0),
            1
        ).otherwise(0)
    )

    # A new cut starts when the preceding window was not a cut.
    movement = movement.withColumn(
        "previous_candidate",
        F.lag("cut_candidate", 1, 0).over(window)
    )

    movement = movement.withColumn(
        "sharp_cut",
        F.when(
            (F.col("cut_candidate") == 1)
            & (F.col("previous_candidate") == 0),
            1
        ).otherwise(0)
    )

    # Aggregate tracking records into one row per player-play
    features = movement.groupBy("PlayKey").agg(
        (F.max("time") - F.min("time")).alias("duration_s"),
        F.sum("dis").alias("distance_yd"),
        F.max("s").alias("max_speed"),
        F.avg("s").alias("avg_speed"),
        F.greatest(
            F.max("acceleration"), F.lit(0.0)
        ).alias("max_accel"),
        F.sum("sharp_cut").cast("int").alias("sharp_cuts")
    )

    # Add surface, position, player, game, and injury information
    play_features = (
        features
        .join(play_info, "PlayKey", "inner")
        .join(injured_plays, "PlayKey", "left")
        .select(
            F.col("PlayKey").alias("play_key"),
            F.col("PlayerKey").cast("int").alias("player_key"),
            F.col("GameID").alias("game_id"),
            F.coalesce(
                F.col("FieldType"), F.lit("unknown")
            ).alias("field_type"),
            F.coalesce(
                F.col("RosterPosition"), F.lit("unknown")
            ).alias("roster_position"),
            "duration_s",
            "distance_yd",
            "max_speed",
            "avg_speed",
            "max_accel",
            F.coalesce(
                F.col("sharp_cuts"), F.lit(0)
            ).alias("sharp_cuts"),
            F.coalesce(
                F.col("is_injury"), F.lit(False)
            ).alias("is_injury")
        )
        .filter(
            F.col("player_key").isNotNull()
            & F.col("game_id").isNotNull()
        )
    )

    # Produce average movement statistics by surface and position
    movement_summary = (
        play_features
        .groupBy("field_type", "roster_position")
        .agg(
            F.count("*").cast("int").alias("plays"),
            F.avg("max_speed").alias("avg_max_speed"),
            F.avg("max_accel").alias("avg_max_accel"),
            F.avg("sharp_cuts").alias("avg_sharp_cuts")
        )
    )

    return {"play_features": play_features, "movement_summary": movement_summary}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=os.path.expanduser("~/nfl_data/clean"))
    args = parser.parse_args(argv)
    from pyspark.storagelevel import StorageLevel
    from common import get_spark, write_table

    spark, play_features = None, None
    try:
        spark = get_spark("NFL_Movement_Features")
        outputs = build_features(
            spark=spark,
            tracking=spark.read.parquet(os.path.join(args.data_root, "kaggle_tracking")),
            plays=spark.read.parquet(os.path.join(args.data_root, "kaggle_plays")),
            injuries=spark.read.parquet(os.path.join(args.data_root, "kaggle_injuries")),
        )
        # Preserve the full job's reuse across previews and final JDBC writes.
        play_features = outputs["play_features"].persist(StorageLevel.MEMORY_AND_DISK)
        movement_summary = outputs["movement_summary"]
        print("\n===== PLAY FEATURES =====")
        play_features.show(10, truncate=False)
        print("\n===== MOVEMENT SUMMARY =====")
        movement_summary.orderBy("field_type", "roster_position").show(100, truncate=False)
        write_table(play_features, "play_features")
        write_table(movement_summary, "movement_summary")
        print("\nMovement features saved successfully.")
    finally:
        try:
            if play_features is not None:
                play_features.unpersist(blocking=True)
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
