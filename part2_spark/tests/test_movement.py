"""Correctness fixtures only; never performance evidence or final-table writes."""

import importlib
import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import benchmark_movement
import movement_adapter
from timing_support import OUTPUT_COLUMNS, validate_adapter_file


class EntryPointTests(unittest.TestCase):
    def test_imports_need_no_spark_or_shared_helpers(self):
        with patch.dict(sys.modules, {"pyspark": None, "common": None}):
            for filename in ("02_movement_features.py", "movement_adapter.py"):
                path = Path(movement_adapter.__file__).with_name(filename)
                spec = importlib.util.spec_from_file_location("import_safety_fixture", path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.assertTrue(callable(module.build_features))
        self.assertEqual(validate_adapter_file(Path(movement_adapter.__file__)),
                         Path(movement_adapter.__file__).resolve())

    def test_adapter_and_full_job_use_shared_function_and_cleanup(self):
        job = importlib.import_module("02_movement_features")
        frames = {name: Mock() for name in OUTPUT_COLUMNS}
        spark = Mock()
        writer = Mock()
        common = types.ModuleType("common")
        common.get_spark, common.write_table = Mock(return_value=spark), writer
        storage = types.ModuleType("pyspark.storagelevel")
        storage.StorageLevel = types.SimpleNamespace(MEMORY_AND_DISK="fixture_storage")
        inputs = dict(spark=spark, tracking=object(), plays=object(), injuries=object())
        with patch.object(job, "build_features", return_value=frames) as build:
            self.assertIs(movement_adapter.build_features(**inputs), frames)
            build.assert_called_once_with(**inputs)
            with patch.dict(sys.modules, {"common": common, "pyspark.storagelevel": storage}):
                job.main(["--data-root", "/fixture"])
            self.assertEqual(build.call_count, 2)
            self.assertIs(build.call_args.kwargs["spark"], spark)
        cached = frames["play_features"].persist.return_value
        cached.unpersist.assert_called_once_with(blocking=True)
        spark.stop.assert_called_once()
        self.assertEqual([call.args[1] for call in writer.call_args_list],
                         ["play_features", "movement_summary"])

    def test_full_job_stops_spark_after_failure(self):
        job = importlib.import_module("02_movement_features")
        spark, features = Mock(), Mock()
        features.persist.return_value = features
        common = types.ModuleType("common")
        common.get_spark = Mock(return_value=spark)
        common.write_table = Mock(side_effect=RuntimeError("fixture write failure"))
        storage = types.ModuleType("pyspark.storagelevel")
        storage.StorageLevel = types.SimpleNamespace(MEMORY_AND_DISK="fixture_storage")
        with patch.dict(sys.modules, {"common": common, "pyspark.storagelevel": storage}), \
             patch.object(job, "build_features", return_value={
                 "play_features": features, "movement_summary": Mock()}):
            with self.assertRaisesRegex(RuntimeError, "fixture write failure"):
                job.main([])
        features.unpersist.assert_called_once_with(blocking=True)
        spark.stop.assert_called_once()


@unittest.skipUnless(os.environ.get("PIVOT_SPARK_TESTS") == "1",
                     "Run PIVOT_SPARK_TESTS=1 in the server's existing Spark environment")
class SparkMovementFixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("Pivot_Movement_Fixtures")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_boundaries_formulas_schema_validation_and_repeated_calls(self):
        from pyspark.sql import functions as F
        rows = []
        def observation(key, time, event=None, speed=2.0, direction=0.0, distance=1.0):
            rows.append((key, float(time), event, float(distance), float(speed), float(direction)))
        # A includes pre/post-play outliers, an early end, and multiple later ends.
        observation("A", -1, distance=100)
        observation("A", -.5, "tackle", distance=100)
        for i in range(7):
            observation("A", i / 10, "ball_snap" if i == 0 else "tackle" if i == 6 else None,
                        speed=i + 1, direction=90 if i >= 5 else 0)
        observation("A", .7, distance=100)
        observation("A", .8, "touchdown", distance=100)
        for key in ("unknown-meta", "bad-player", "bad-game", "missing-meta"):
            observation(key, 0, "kickoff")
            observation(key, .1, "touchback")
        observation("no-start", 0)
        observation("no-start", 1, "tackle")
        observation("no-end", 0, "ball_snap")
        observation("no-end", 1)
        observation("early-only", 0, "tackle")
        observation("early-only", 1, "snap_direct")
        observation("same-end", 0, "free_kick")
        observation("same-end", 0, "tackle")
        tracking = self.spark.createDataFrame(rows, "PlayKey string, time double, event string, dis double, s double, dir double")
        metadata = [(key, "7", "G", "Natural", "WR") for key in
                    ("A", "no-start", "no-end", "early-only", "same-end")]
        metadata += [("unknown-meta", "8", "G", None, None),
                     ("bad-player", None, "G", "Natural", "WR"),
                     ("bad-game", "9", None, "Natural", "WR")]
        plays = self.spark.createDataFrame(metadata, "PlayKey string, PlayerKey string, GameID string, FieldType string, RosterPosition string")
        injuries = self.spark.createDataFrame([("A",), ("A",), (None,)], "PlayKey string")
        arguments = dict(spark=self.spark, tracking=tracking, plays=plays, injuries=injuries)
        outputs = movement_adapter.build_features(**arguments)
        for name, columns in OUTPUT_COLUMNS.items():
            self.assertEqual(tuple(outputs[name].columns), columns)
        types_by_name = dict(outputs["play_features"].dtypes)
        self.assertEqual(types_by_name, dict(zip(OUTPUT_COLUMNS["play_features"],
            ("string", "int", "string", "string", "string", "double", "double",
             "double", "double", "double", "int", "boolean"))))
        self.assertEqual(dict(outputs["movement_summary"].dtypes), dict(zip(
            OUTPUT_COLUMNS["movement_summary"], ("string", "string", "int", "double", "double", "double"))))
        first = {row.play_key: row.asDict() for row in outputs["play_features"].collect()}
        self.assertEqual(set(first), {"A", "unknown-meta"})
        for name, expected in (("duration_s", .6), ("distance_yd", 7), ("max_speed", 7),
                               ("avg_speed", 4), ("max_accel", 10), ("sharp_cuts", 1)):
            self.assertAlmostEqual(first["A"][name], expected)
        self.assertTrue(first["A"]["is_injury"])
        self.assertFalse(first["unknown-meta"]["is_injury"])
        self.assertEqual(first["unknown-meta"]["field_type"], "unknown")
        self.assertEqual(first["unknown-meta"]["roster_position"], "unknown")
        summary = outputs["movement_summary"].collect()
        self.assertEqual(sum(row.plays for row in summary), 2)
        # Use actual fixture frames through the validator's read interface.
        # No JDBC connection or writes occur in this test.
        def validate(frames):
            reader = types.SimpleNamespace(parquet=lambda path: frames[Path(path).name])
            return benchmark_movement.validate_outputs(
                types.SimpleNamespace(read=reader), Path("fixture"), F, tracking, 9)
        invalid_cases = [
            ("play_features", outputs["play_features"].unionByName(outputs["play_features"]), "Duplicate"),
            ("play_features", outputs["play_features"].withColumn("game_id", F.lit(None).cast("string")), "Null required"),
            ("play_features", outputs["play_features"].withColumn("play_key", F.concat(F.lit("outside-"), F.col("play_key"))), "outside"),
            ("movement_summary", outputs["movement_summary"].withColumn("plays", F.col("plays") + 1), "do not agree"),
            ("movement_summary", outputs["movement_summary"].filter(F.col("field_type") == "Natural"), "do not agree"),
        ]
        # Cache only these synthetic outputs during validator fault injection.
        for frame in outputs.values():
            frame.cache().count()
        try:
            self.assertEqual(validate(outputs), {"play_features": 2, "movement_summary": 2})
            for name, invalid, message in invalid_cases:
                with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                    validate({**outputs, name: invalid})
        finally:
            for frame in outputs.values():
                frame.unpersist(blocking=True)
        again = movement_adapter.build_features(**arguments)
        second = {row.play_key: row.asDict() for row in again["play_features"].collect()}
        self.assertEqual(first, second)
        self.assertFalse(again["play_features"].is_cached)
        self.assertFalse(again["movement_summary"].is_cached)
        self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(), 0)


if __name__ == "__main__":
    unittest.main()
