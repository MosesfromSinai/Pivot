"""Spark-side benchmark driver. Run through run_timing.py on the Linux server."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import socket
import time

from timing_support import (
    DEFAULT_SEED, FORMAT_VERSION, HASH_BUCKETS, OUTPUT_COLUMNS, code_provenance,
    ensure_paired_subsets, positive_fraction, read_json, sample_threshold, utc_now,
    validate_adapter_file, write_json,
)


def input_manifest(data_root):
    """Detect changed local input files without reading their data or secrets."""
    files = []
    for dataset in ("kaggle_tracking", "kaggle_plays", "kaggle_injuries"):
        directory = data_root / dataset
        if not directory.is_dir():
            raise ValueError(f"Missing cleaned dataset directory: {directory}")
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                stat = path.stat()
                files.append((str(path.relative_to(data_root)), stat.st_size, stat.st_mtime_ns))
    encoded = json.dumps(files, separators=(",", ":")).encode()
    return {"metadata_sha256": hashlib.sha256(encoded).hexdigest(),
            "files": len(files), "bytes": sum(row[1] for row in files)}


def block_adapter_write(*args, **kwargs):
    raise RuntimeError(
        "Benchmark adapters must not call common.write_table. "
        "The driver alone appends validated timing measurements."
    )


def load_adapter(path):
    path = validate_adapter_file(path)
    spec = importlib.util.spec_from_file_location("pivot_movement_benchmark_adapter", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if module.BENCHMARK_ADAPTER_READY is not True or module.BENCHMARK_ADAPTER_VERSION != 1:
        raise ValueError("Adapter readiness changed during import.")
    return module.build_features


def check_master(spark, expected):
    actual = spark.sparkContext.master
    if actual != expected:
        raise RuntimeError(f"Requested master {expected}, but Spark is using {actual}.")
    return actual


def select_tracking(tracking, fraction, seed, functions):
    """Stable nested buckets of entire PlayKey trajectories, never row sampling."""
    if "PlayKey" not in tracking.columns:
        raise ValueError("Tracking data has no PlayKey column.")
    if fraction == 1.0:
        return tracking
    bucket = functions.pmod(
        functions.xxhash64(functions.lit(seed), functions.col("PlayKey").cast("string")),
        functions.lit(HASH_BUCKETS),
    )
    return tracking.filter(bucket < functions.lit(sample_threshold(fraction)))


def validate_outputs(spark, output_root, functions, tracking, selected_plays):
    counts = {}
    keys = {"play_features": ("play_key",),
            "movement_summary": ("field_type", "roster_position")}
    for name, primary_key in keys.items():
        saved = spark.read.parquet(str(output_root / name))
        counts[name] = saved.count()
        if counts[name] <= 0:
            raise ValueError(f"Benchmark output {name} is empty.")
        for key in primary_key:
            if saved.filter(functions.col(key).isNull()).limit(1).count():
                raise ValueError(f"Null primary key {key} in {name}.")
        duplicates = saved.groupBy(*primary_key).count().filter(functions.col("count") > 1)
        if duplicates.limit(1).count():
            raise ValueError(f"Duplicate primary keys in {name}.")
        if name == "play_features":
            if counts[name] != selected_plays:
                raise ValueError("play_features must contain one row for every selected PlayKey.")
            selected_keys = tracking.select(
                functions.col("PlayKey").cast("string").alias("play_key")
            ).distinct()
            unexpected = saved.select("play_key").join(selected_keys, ["play_key"], "left_anti")
            if unexpected.limit(1).count():
                raise ValueError("play_features contains a play outside the selected tracking subset.")
            for column in ("player_key", "game_id", "field_type", "is_injury"):
                if saved.filter(functions.col(column).isNull()).limit(1).count():
                    raise ValueError(f"Null required field {column} in play_features.")
    return counts


def append_measurement(spark, helper, record):
    # Match schema.sql exactly; leave auto-generated run_id and run_at to MySQL.
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

    schema = StructType([
        StructField("workers", IntegerType(), False),
        StructField("data_fraction", DoubleType(), False),
        StructField("rows_processed", LongType(), False),
        StructField("seconds", DoubleType(), False),
    ])
    values = [(record["workers"], float(record["data_fraction"]),
               record["rows_processed"], float(record["seconds"]))]
    helper(spark.createDataFrame(values, schema).coalesce(1), "timing_results", mode="append")


def safe_error(error):
    message = f"{type(error).__name__}: {error}"
    for name in ("MYSQL_PASSWORD", "MYSQL_URL"):
        secret = os.environ.get(name)
        if secret:
            message = message.replace(secret, "[redacted]")
    return message


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--adapter", required=True, type=Path)
    result.add_argument("--data-root", required=True, type=Path)
    result.add_argument("--run-dir", required=True, type=Path)
    result.add_argument("--suite-id", required=True)
    result.add_argument("--run-id", required=True)
    result.add_argument("--workers", required=True, type=int, choices=(1, 2))
    result.add_argument("--fraction", required=True, type=positive_fraction)
    result.add_argument("--seed", type=int, default=DEFAULT_SEED)
    result.add_argument("--record-timing", action="store_true")
    result.add_argument("--reference-result", type=Path)
    return result


def run(args):
    record = {
        "format_version": FORMAT_VERSION, "suite_id": args.suite_id,
        "run_id": args.run_id, "fixture": False,
        "kind": "required" if args.record_timing else "smoke",
        "workers": args.workers, "master": f"local[{args.workers}]",
        "data_fraction": args.fraction, "seed": args.seed,
        "hash_buckets": HASH_BUCKETS, "started_at": utc_now(),
        "status": "running", "phase": "preflight", "appended": False,
        "data_root": str(args.data_root.resolve()),
        "host": socket.gethostname(), "python": platform.python_version(),
    }
    spark, common, original_write = None, None, None
    append_started = False
    result_path = args.run_dir / "result.json"
    # Exclusive marker prevents accidental duplicate execution or metadata replacement.
    with (args.run_dir / "worker_started.json").open("x", encoding="utf-8") as stream:
        json.dump({"started_at": record["started_at"]}, stream)
    try:
        record["code"] = code_provenance(args.adapter)
        before = input_manifest(args.data_root)
        record["input_manifest"] = before
        import common
        from pyspark.sql import functions as F

        original_write = common.write_table
        # Installed BEFORE adapter import, so `from common import write_table` is guarded.
        common.write_table = block_adapter_write
        build_features = load_adapter(args.adapter)
        output_root = args.run_dir / "outputs"
        output_root.mkdir(exist_ok=False)
        record["phase"] = "startup"
        started = time.perf_counter()
        spark = common.get_spark("Pivot_Adolfo_Movement_Timing")
        record["startup_seconds"] = time.perf_counter() - started
        check_master(spark, record["master"])
        record["spark_version"] = spark.version
        record["spark_application_id"] = spark.sparkContext.applicationId
        record["spark_settings"] = {
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        }

        record["phase"] = "measurement"
        started = time.perf_counter()
        tracking = select_tracking(
            spark.read.parquet(str(args.data_root / "kaggle_tracking")), args.fraction, args.seed, F
        )
        plays = spark.read.parquet(str(args.data_root / "kaggle_plays"))
        injuries = spark.read.parquet(str(args.data_root / "kaggle_injuries"))
        outputs = build_features(spark=spark, tracking=tracking, plays=plays, injuries=injuries)
        if not isinstance(outputs, dict) or set(outputs) != set(OUTPUT_COLUMNS):
            raise ValueError("Adapter must return exactly play_features and movement_summary frames.")
        check_master(spark, record["master"])
        for name, columns in OUTPUT_COLUMNS.items():
            if set(outputs[name].columns) != set(columns):
                raise ValueError(f"Adapter output columns do not match schema.sql for {name}.")
            # Writing every output column forces the real movement expressions to execute.
            outputs[name].select(*columns).write.mode("errorifexists").parquet(str(output_root / name))
        record["seconds"] = time.perf_counter() - started

        record["phase"] = "validation"
        check_master(spark, record["master"])
        statistics = tracking.agg(
            F.count("*").alias("rows"),
            F.countDistinct("PlayKey").alias("plays"),
            F.sum(F.when(F.col("PlayKey").isNull(), 1).otherwise(0)).alias("null_keys"),
            F.sum(F.xxhash64(*[F.col(c) for c in tracking.columns]).cast("decimal(38,0)"))
            .alias("fingerprint"),
        ).first()
        record["rows_processed"] = int(statistics["rows"])
        record["selected_plays"] = int(statistics["plays"])
        record["subset_fingerprint"] = str(statistics["fingerprint"])
        if record["rows_processed"] <= 0 or statistics["null_keys"]:
            raise ValueError("Tracking subset is empty or contains null PlayKey values.")
        record["output_rows"] = validate_outputs(
            spark, output_root, F, tracking, record["selected_plays"]
        )
        if input_manifest(args.data_root) != before:
            raise ValueError("Cleaned input file metadata changed during the benchmark.")
        if code_provenance(args.adapter)["source_sha256"] != record["code"]["source_sha256"]:
            raise ValueError("Benchmark source changed during the run.")
        record["validated_at"] = utc_now()
        record["status"] = "validated"
        if args.reference_result:
            ensure_paired_subsets([read_json(args.reference_result), record])
        write_json(result_path, record)

        if args.record_timing:
            record["phase"] = "append"
            # Persist intent before JDBC; failures or interruptions may leave a committed row.
            record["status"] = "append_pending"
            write_json(result_path, record)
            append_started = True
            append_measurement(spark, original_write, record)
            record["appended"] = True
        record.update(status="success", phase="complete", finished_at=utc_now())
        write_json(result_path, record)
        print(json.dumps({k: record[k] for k in (
            "run_id", "master", "data_fraction", "rows_processed", "seconds", "appended"
        )}), flush=True)
        return 0
    except Exception as error:
        record.update(status="append_uncertain" if append_started else "failed",
                      error=safe_error(error), finished_at=utc_now())
        write_json(result_path, record)
        print(record["error"], flush=True)
        return 1
    finally:
        if common is not None and original_write is not None:
            common.write_table = original_write
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                print("Spark shutdown warning: " + safe_error(error), flush=True)


if __name__ == "__main__":
    raise SystemExit(run(parser().parse_args()))
