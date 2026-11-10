"""Local logic tests with synthetic fixtures; these NEVER measure Spark performance."""

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import benchmark_movement
import plot_timing
import run_timing
from timing_support import (
    assert_matching_result, configurations, ensure_paired_subsets,
    sample_threshold, validate_adapter_file,
)

FIXTURE = Path(__file__).parent / "fixtures" / "timing_results.json"


class ConfigurationTests(unittest.TestCase):
    def test_sampling_keeps_complete_trajectories_at_boundary_buckets(self):
        # Synthetic expression evaluator checks grouping/boundaries, not Spark's hash algorithm.
        class Expression:
            def __init__(self, evaluate): self.evaluate = evaluate
            def cast(self, kind): return self
            def __lt__(self, other):
                return Expression(lambda row: self.evaluate(row) < other.evaluate(row))
        class Functions:
            @staticmethod
            def lit(value): return Expression(lambda row: value)
            @staticmethod
            def col(name): return Expression(lambda row: row[name])
            @staticmethod
            def xxhash64(seed, key):
                return Expression(lambda row: {"A": 0, "B": 250000, "C": 500000, "D": 750000}[key.evaluate(row)])
            @staticmethod
            def pmod(value, modulus):
                return Expression(lambda row: value.evaluate(row) % modulus.evaluate(row))
        class Tracking:
            columns = ["PlayKey", "time"]
            def __init__(self, rows): self.rows = rows
            def filter(self, condition):
                return Tracking([r for r in self.rows if condition.evaluate(r)])
        tracking = Tracking([{"PlayKey": key, "time": t} for key in "ABCD" for t in (0, 1, 2)])
        for fraction, keys in ((.25, "A"), (.5, "AB"), (.75, "ABC"), (1.0, "ABCD")):
            selected = benchmark_movement.select_tracking(tracking, fraction, 179, Functions)
            self.assertEqual({r["PlayKey"] for r in selected.rows}, set(keys))
            self.assertEqual(len(selected.rows), len(keys) * 3)

    def test_required_matrix_and_separate_smoke(self):
        configs = configurations()
        self.assertEqual(len(configs), 8)
        self.assertEqual(len({c["run_id"] for c in configs}), 8)
        self.assertEqual({(c["workers"], c["data_fraction"]) for c in configs},
                         {(w, f) for w in (1, 2) for f in (0.25, 0.5, 0.75, 1.0)})
        self.assertEqual(configurations(True)[0]["data_fraction"], 0.01)

    def test_invalid_fraction_and_fixed_nested_thresholds(self):
        for fraction in (0, -1, 1.1, float("nan"), float("inf"), 0.0000001):
            with self.subTest(fraction=fraction), self.assertRaises(ValueError):
                sample_threshold(fraction)
        self.assertEqual([sample_threshold(f) for f in (.25, .5, .75, 1)],
                         [250000, 500000, 750000, 1000000])

    def test_template_cannot_launch_a_benchmark(self):
        with self.assertRaisesRegex(ValueError, "not ready"):
            validate_adapter_file(Path(run_timing.__file__).with_name("movement_adapter_template.py"))

    def test_wrong_master_fails(self):
        with self.assertRaisesRegex(RuntimeError, "Spark is using"):
            benchmark_movement.check_master(types.SimpleNamespace(
                sparkContext=types.SimpleNamespace(master="local[2]")), "local[1]")


class PersistenceTests(unittest.TestCase):
    def test_timing_helper_uses_append_and_explicit_sql_types(self):
        class IntegerType: pass
        class DoubleType: pass
        class LongType: pass
        class StructField:
            def __init__(self, name, kind, nullable):
                self.name, self.kind, self.nullable = name, kind, nullable
        class StructType(list): pass
        fake_types = types.ModuleType("pyspark.sql.types")
        for name, value in locals().copy().items():
            if name in ("IntegerType", "DoubleType", "LongType", "StructField", "StructType"):
                setattr(fake_types, name, value)
        spark, helper = Mock(), Mock()
        record = {"workers": 2, "data_fraction": .5, "rows_processed": 3_000_000_000, "seconds": 2.5}
        with patch.dict(sys.modules, {"pyspark.sql.types": fake_types}):
            benchmark_movement.append_measurement(spark, helper, record)
        values, schema = spark.createDataFrame.call_args.args
        self.assertEqual(values, [(2, .5, 3_000_000_000, 2.5)])
        self.assertEqual([(f.name, type(f.kind).__name__) for f in schema], [
            ("workers", "IntegerType"), ("data_fraction", "DoubleType"),
            ("rows_processed", "LongType"), ("seconds", "DoubleType")])
        helper.assert_called_once_with(spark.createDataFrame.return_value.coalesce.return_value,
                                       "timing_results", mode="append")

    def test_adapter_cannot_use_shared_table_writer(self):
        for name in ("play_features", "movement_summary", "timing_results"):
            with self.subTest(table=name), self.assertRaises(RuntimeError):
                benchmark_movement.block_adapter_write(Mock(), name)

    def test_input_metadata_detects_changed_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("kaggle_tracking", "kaggle_plays", "kaggle_injuries"):
                (root / name).mkdir()
            source = root / "kaggle_tracking" / "fixture.parquet"
            source.write_bytes(b"not real parquet: metadata test only")
            before = benchmark_movement.input_manifest(root)
            source.write_bytes(b"changed synthetic bytes")
            self.assertNotEqual(before, benchmark_movement.input_manifest(root))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.suite = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def save_and_load(self, suite, **kwargs):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text(json.dumps(suite), encoding="utf-8")
            return plot_timing.load_measurements(path, **kwargs)

    def test_fixtures_require_explicit_opt_in(self):
        with self.assertRaisesRegex(ValueError, "Fixture"):
            plot_timing.load_measurements(FIXTURE)
        _, records, complete = plot_timing.load_measurements(FIXTURE, allow_fixtures=True)
        self.assertTrue(complete)
        self.assertEqual(len(plot_timing.table_rows(records)), 4)
        self.assertEqual(plot_timing.table_rows(records)[0]["speedup_1_over_2"], 4 / 3)

    def test_mismatched_subsets_and_provenance_are_rejected(self):
        for field, value in (("rows_processed", 999), ("subset_fingerprint", "different"),
                             ("seed", 999), ("spark_version", "different")):
            suite = copy.deepcopy(self.suite)
            suite["runs"][1][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.save_and_load(suite, allow_fixtures=True)

    def test_duplicate_or_wrong_master_or_nonfinite_evidence_rejected(self):
        for change in ("duplicate", "master", "nan", "mixed"):
            suite = copy.deepcopy(self.suite)
            if change == "duplicate":
                suite["runs"].append(suite["runs"][0])
            elif change == "master":
                suite["runs"][0]["master"] = "local[8]"
            elif change == "nan":
                suite["runs"][0]["seconds"] = float("nan")
            else:
                suite["runs"][0]["fixture"] = False
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.save_and_load(suite, allow_fixtures=True)

    def test_partial_requires_opt_in_and_smoke_never_becomes_report(self):
        self.suite["runs"].pop()
        with self.assertRaises(ValueError):
            self.save_and_load(self.suite, allow_fixtures=True)
        _, _, complete = self.save_and_load(self.suite, allow_fixtures=True, allow_partial=True)
        self.assertFalse(complete)
        self.suite["kind"] = "smoke"
        with self.assertRaisesRegex(ValueError, "Smoke"):
            self.save_and_load(self.suite, allow_fixtures=True, allow_partial=True)

    def test_worker_result_cannot_be_stale_or_fixture(self):
        record = copy.deepcopy(self.suite["runs"][0])
        config = configurations()[0]
        with self.assertRaisesRegex(ValueError, "fixture"):
            assert_matching_result(record, config, self.suite["suite_id"])
        record["fixture"] = False  # Simulated server response, only in this unit test.
        assert_matching_result(record, config, self.suite["suite_id"])
        record["suite_id"] = "stale"
        with self.assertRaisesRegex(ValueError, "suite_id"):
            assert_matching_result(record, config, self.suite["suite_id"])


class RunnerTests(unittest.TestCase):
    def args(self, root, smoke=False):
        adapter = root / "fixture_adapter.py"
        adapter.write_text("BENCHMARK_ADAPTER_VERSION = 1\nBENCHMARK_ADAPTER_READY = True\n"
                           "def build_features(**kwargs): raise AssertionError('fixture only')\n")
        for name in ("kaggle_tracking", "kaggle_plays", "kaggle_injuries"):
            (root / "clean" / name).mkdir(parents=True)
        return argparse.Namespace(adapter=adapter, output_dir=root / "results", data_root=root / "clean",
                                  spark_submit="spark-submit", seed=179, smoke=smoke, plan=False,
                                  continue_on_error=False)

    def test_launch_error_stops_matrix_and_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.args(Path(directory))
            with patch("run_timing.shutil.which", return_value="FAKE-spark-submit"), \
                 patch("run_timing.code_provenance", return_value={"source_sha256": {}}), \
                 patch("run_timing.subprocess.run", return_value=subprocess.CompletedProcess([], 1)) as launch:
                self.assertEqual(run_timing.execute_suite(args), 1)
            self.assertEqual(launch.call_count, 1)
            suite = json.loads((args.output_dir / "summary.json").read_text())
            self.assertEqual(suite["status"], "failed")
            self.assertEqual(len(suite["runs"]), 1)
            self.assertTrue((args.output_dir / suite["runs"][0]["run_id"] / "run.log").exists())

    def test_sequential_smoke_uses_master_and_never_requests_insert(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.args(Path(directory), smoke=True)
            def fake_launch(command, **kwargs):
                # Synthetic response for runner logic; NOT a Spark execution.
                run_dir = Path(command[command.index("--run-dir") + 1])
                suite_id = command[command.index("--suite-id") + 1]
                record = {**configurations(True)[0], "fixture": False, "format_version": 1,
                          "suite_id": suite_id, "status": "success", "rows_processed": 10,
                          "seconds": 1.0, "appended": False}
                (run_dir / "result.json").write_text(json.dumps(record))
                return subprocess.CompletedProcess(command, 0)
            with patch("run_timing.shutil.which", return_value="FAKE-spark-submit"), \
                 patch("run_timing.code_provenance", return_value={"source_sha256": {}}), \
                 patch("run_timing.subprocess.run", side_effect=fake_launch) as launch:
                self.assertEqual(run_timing.execute_suite(args), 0)
            command = launch.call_args.args[0]
            self.assertEqual(command[command.index("--master") + 1], "local[1]")
            self.assertNotIn("--record-timing", command)
            with self.assertRaises(FileExistsError):
                with patch("run_timing.shutil.which", return_value="FAKE-spark-submit"), \
                     patch("run_timing.code_provenance", return_value={"source_sha256": {}}):
                    run_timing.execute_suite(args)

    def test_all_eight_commands_reference_previous_validated_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.args(Path(directory))
            fixture_records = json.loads(FIXTURE.read_text())["runs"]
            def fake_launch(command, **kwargs):
                # This is mocked driver output, never a benchmark result.
                run_dir = Path(command[command.index("--run-dir") + 1])
                run_id = command[command.index("--run-id") + 1]
                record = copy.deepcopy(next(r for r in fixture_records if r["run_id"] == run_id))
                record.update(fixture=False, appended=True,
                              suite_id=command[command.index("--suite-id") + 1])
                (run_dir / "result.json").write_text(json.dumps(record))
                return subprocess.CompletedProcess(command, 0)
            with patch("run_timing.shutil.which", return_value="FAKE-spark-submit"), \
                 patch("run_timing.code_provenance", return_value={"source_sha256": {}}), \
                 patch("run_timing.subprocess.run", side_effect=fake_launch) as launch:
                self.assertEqual(run_timing.execute_suite(args), 0)
            self.assertEqual(launch.call_count, 8)
            for index, call in enumerate(launch.call_args_list):
                command = call.args[0]
                self.assertIn("--record-timing", command)
                self.assertEqual("--reference-result" in command, index > 0)
                if index > 0:
                    reference = Path(command[command.index("--reference-result") + 1])
                    self.assertTrue(reference.is_file())

    def test_uncertain_append_is_preserved_and_never_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.args(Path(directory))
            def fake_launch(command, **kwargs):
                run_dir = Path(command[command.index("--run-dir") + 1])
                (run_dir / "result.json").write_text(json.dumps({
                    **configurations()[0], "fixture": False, "status": "append_uncertain",
                    "appended": False, "phase": "append", "seconds": 4.0,
                    "error": "Synthetic JDBC acknowledgment failure",
                }))
                return subprocess.CompletedProcess(command, 1)
            with patch("run_timing.shutil.which", return_value="FAKE-spark-submit"), \
                 patch("run_timing.code_provenance", return_value={"source_sha256": {}}), \
                 patch("run_timing.subprocess.run", side_effect=fake_launch) as launch:
                self.assertEqual(run_timing.execute_suite(args), 1)
            self.assertEqual(launch.call_count, 1)
            suite = json.loads((args.output_dir / "summary.json").read_text())
            self.assertEqual(suite["runs"][0]["status"], "append_uncertain")
            self.assertEqual(suite["runs"][0]["seconds"], 4.0)


@unittest.skipUnless(importlib.util.find_spec("matplotlib"), "matplotlib not installed; runner logic still tested")
class PlotRenderingTests(unittest.TestCase):
    def test_fixture_export_creates_labeled_graphs_and_table(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "plot"
            plot_timing.export_results(FIXTURE, output, allow_fixtures=True)
            self.assertGreater((output / "timing_plot.png").stat().st_size, 1000)
            self.assertIn("FIXTURE ONLY", (output / "timing_plot.svg").read_text(encoding="utf-8"))
            self.assertIn("FIXTURE ONLY", (output / "timing_table.md").read_text(encoding="utf-8"))
            with self.assertRaises(FileExistsError):
                plot_timing.export_results(FIXTURE, output, allow_fixtures=True)


if __name__ == "__main__":
    unittest.main()
