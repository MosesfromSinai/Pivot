"""Standard-library helpers shared by the timing runner and Spark driver."""

import ast
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
from datetime import datetime, timezone

FORMAT_VERSION = 1
FRACTIONS = (0.25, 0.50, 0.75, 1.0)
WORKERS = (1, 2)
HASH_BUCKETS = 1_000_000
DEFAULT_SEED = 179
JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"
OUTPUT_COLUMNS = {
    "play_features": (
        "play_key", "player_key", "game_id", "field_type", "roster_position",
        "duration_s", "distance_yd", "max_speed", "avg_speed", "max_accel",
        "sharp_cuts", "is_injury",
    ),
    "movement_summary": (
        "field_type", "roster_position", "plays", "avg_max_speed",
        "avg_max_accel", "avg_sharp_cuts",
    ),
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def positive_fraction(value):
    value = float(value)
    if not math.isfinite(value) or not 0 < value <= 1:
        raise ValueError("Data fraction must be greater than zero and at most one.")
    if value * HASH_BUCKETS < 1:
        raise ValueError("Data fraction is smaller than one sampling bucket.")
    return value


def sample_threshold(fraction):
    return round(positive_fraction(fraction) * HASH_BUCKETS)


def configurations(smoke=False):
    fractions, workers = ((0.01,), (1,)) if smoke else (FRACTIONS, WORKERS)
    return [
        {"run_id": f"f{round(f * 100):03d}-local{w}",
         "workers": w, "data_fraction": f, "master": f"local[{w}]"}
        for f in fractions for w in workers
    ]


def write_json(path, data):
    """Replace a metadata file atomically; output directories are exclusive."""
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, allow_nan=False)
        stream.write("\n")
    os.replace(temporary, path)


def read_json(path):
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def validate_adapter_file(path):
    """Check readiness without importing PySpark or executing the adapter."""
    path = Path(path).resolve(strict=True)
    if path.suffix != ".py":
        raise ValueError("The adapter must be a Python source file.")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    declarations = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    declarations[target.id] = node.value.value
    if declarations.get("BENCHMARK_ADAPTER_READY") is not True:
        raise ValueError(
            "Movement adapter is not ready. Connect Joshua's real calculations "
            "and safe output behavior before setting BENCHMARK_ADAPTER_READY = True."
        )
    if declarations.get("BENCHMARK_ADAPTER_VERSION") != 1:
        raise ValueError("Expected BENCHMARK_ADAPTER_VERSION = 1.")
    if not any(isinstance(n, ast.FunctionDef) and n.name == "build_features"
               for n in tree.body):
        raise ValueError("Adapter must define build_features as described in TIMING.md.")
    return path


def code_provenance(adapter=None):
    directory = Path(__file__).resolve().parent
    def git(*args):
        result = subprocess.run(
            ["git", *args], cwd=directory, capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    sources = sorted(directory.glob("*.py"))
    if adapter is not None and Path(adapter) not in sources:
        sources.append(Path(adapter))
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    try:
        commit, status = git("rev-parse", "HEAD"), git("status", "--porcelain=v1")
    except (OSError, subprocess.CalledProcessError):
        commit, status = None, "Git metadata unavailable"
    return {"git_commit": commit, "git_status": status, "source_sha256": hashes}


def assert_matching_result(record, config, suite_id):
    """Do not accept stale, mismatched, or fixture subprocess output."""
    expected = {
        "format_version": FORMAT_VERSION, "suite_id": suite_id,
        "run_id": config["run_id"], "workers": config["workers"],
        "data_fraction": config["data_fraction"], "master": config["master"],
        "fixture": False,
    }
    for key, value in expected.items():
        if record.get(key) != value:
            raise ValueError(f"Worker result does not match requested {key}.")
    if record.get("status") != "success":
        raise ValueError("Worker did not report a successful validated run.")
    if type(record.get("rows_processed")) is not int or record["rows_processed"] <= 0:
        raise ValueError("Worker did not report a positive tracking-row count.")
    seconds = record.get("seconds")
    if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("Worker did not report a finite positive duration.")


def ensure_paired_subsets(records):
    """Compare measured inputs, not just the requested fractions."""
    seen = {}
    baseline = None
    for record in records:
        if record.get("status") not in ("success", "validated"):
            continue
        provenance = (
            record.get("seed"), record.get("hash_buckets"), record.get("data_root"),
            record.get("input_manifest"), record.get("code", {}).get("source_sha256"),
            record.get("host"), record.get("spark_version"),
            {k: v for k, v in record.get("spark_settings", {}).items()
             if k != "default_parallelism"},
        )
        if baseline is not None and baseline != provenance:
            raise ValueError("Runs have different input, code, seed, host, or Spark provenance.")
        baseline = provenance
        fraction = record["data_fraction"]
        signature = (record["rows_processed"], record.get("selected_plays"),
                     record.get("subset_fingerprint"))
        if fraction in seen and seen[fraction] != signature:
            raise ValueError(f"Input subsets differ between masters at fraction {fraction}.")
        seen[fraction] = signature
