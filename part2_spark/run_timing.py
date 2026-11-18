"""Plan or sequentially launch Adolfo's timing configurations on the server."""

import argparse
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import uuid

from timing_support import (
    DEFAULT_SEED, FORMAT_VERSION, JDBC_PACKAGE, assert_matching_result,
    code_provenance, configurations, ensure_paired_subsets, read_json, utc_now,
    validate_adapter_file, write_json,
)


def build_command(config, args, suite_id, run_dir, reference=None):
    command = [
        args.spark_submit, "--master", config["master"], "--packages", JDBC_PACKAGE,
        str(Path(__file__).with_name("benchmark_movement.py").resolve()),
        "--adapter", str(args.adapter), "--data-root", str(args.data_root),
        "--run-dir", str(run_dir), "--suite-id", suite_id,
        "--run-id", config["run_id"], "--workers", str(config["workers"]),
        "--fraction", str(config["data_fraction"]), "--seed", str(args.seed),
    ]
    if not args.smoke:
        command.append("--record-timing")
    if reference is not None:
        command.extend(["--reference-result", str(reference)])
    return command


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--plan", action="store_true", help="Print matrix only; no Spark or file writes.")
    result.add_argument("--adapter", type=Path, help="Ready adapter wrapping Joshua's actual code.")
    result.add_argument("--data-root", type=Path, default=Path.home() / "nfl_data" / "clean")
    result.add_argument("--output-dir", type=Path, help="New directory outside the cleaned data.")
    result.add_argument("--spark-submit", default="spark-submit")
    result.add_argument("--seed", type=int, default=DEFAULT_SEED)
    result.add_argument("--smoke", action="store_true", help="One 1%%/local[1] run; no MySQL insert.")
    result.add_argument("--continue-on-error", action="store_true", help="Record failures and try later configurations.")
    return result


def execute_suite(args):
    configs = configurations(args.smoke)
    if args.plan:
        print(json.dumps({
            "status": "plan_only", "fixture": False,
            "adapter": str(args.adapter) if args.adapter else "REQUIRED: Joshua-backed adapter",
            "kind": "smoke" if args.smoke else "required", "seed": args.seed,
            "configurations": configs,
        }, indent=2))
        return 0
    if args.adapter is None or args.output_dir is None:
        raise ValueError("Execution requires --adapter and a new --output-dir; use --plan to inspect the matrix.")
    args.adapter = validate_adapter_file(args.adapter)
    args.data_root = args.data_root.expanduser().resolve(strict=True)
    for name in ("kaggle_tracking", "kaggle_plays", "kaggle_injuries"):
        if not (args.data_root / name).is_dir():
            raise ValueError(f"Missing cleaned dataset: {name}")
    args.output_dir = args.output_dir.expanduser().resolve()
    if args.output_dir.is_relative_to(args.data_root) or args.data_root.is_relative_to(args.output_dir):
        raise ValueError("Benchmark output must be separate from the cleaned data tree.")
    executable = shutil.which(args.spark_submit)
    if executable is None:
        raise ValueError("spark-submit was not found. Activate cs179 on the server; do not install Spark locally.")
    args.spark_submit = executable
    suite = {
        "format_version": FORMAT_VERSION, "suite_id": str(uuid.uuid4()), "fixture": False,
        "kind": "smoke" if args.smoke else "required", "seed": args.seed,
        "started_at": utc_now(), "status": "running", "planned": configs, "runs": [],
        "code": code_provenance(args.adapter),
    }
    args.output_dir.mkdir(parents=True, exist_ok=False)
    summary_path = args.output_dir / "summary.json"
    write_json(summary_path, suite)
    references = {}
    try:
        for config in configs:
            if code_provenance(args.adapter)["source_sha256"] != suite["code"]["source_sha256"]:
                raise ValueError("Benchmark source changed after the suite started.")
            run_dir = args.output_dir / config["run_id"]
            run_dir.mkdir(exist_ok=False)
            reference = references.get(config["data_fraction"])
            if reference is None and references:
                reference = next(iter(references.values()))
            command = build_command(config, args, suite["suite_id"], run_dir, reference)
            write_json(run_dir / "command.json", {"argv": command, "started_at": utc_now()})
            print(shlex.join(command), flush=True)
            record = {**config, "fixture": False, "suite_id": suite["suite_id"],
                      "status": "failed", "phase": "launch"}
            try:
                with (run_dir / "run.log").open("x", encoding="utf-8") as log:
                    completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
                result_path = run_dir / "result.json"
                if result_path.is_file():
                    record = read_json(result_path)
                else:
                    record = {**config, "fixture": False, "suite_id": suite["suite_id"],
                              "status": "failed", "phase": "launch",
                              "error": "Spark exited without result.json; inspect run.log."}
                record["exit_code"] = completed.returncode
                if completed.returncode == 0:
                    assert_matching_result(record, config, suite["suite_id"])
                    if record.get("appended") is not (not args.smoke):
                        raise ValueError("Worker append state does not match the suite kind.")
                    ensure_paired_subsets(suite["runs"] + [record])
                    references[config["data_fraction"]] = result_path
                elif record.get("status") == "success":
                    record.update(status="failed", error="Spark exited nonzero after reporting success; inspect the log and MySQL before retrying.")
            except (OSError, ValueError) as error:
                record.update(status="failed", runner_error=str(error))
            suite["runs"].append(record)
            write_json(summary_path, suite)
            if record.get("status") != "success" and not args.continue_on_error:
                break
        suite["status"] = (
            "success" if len(suite["runs"]) == len(configs)
            and all(r.get("status") == "success" for r in suite["runs"]) else "failed"
        )
    except (KeyboardInterrupt, Exception) as error:
        suite.update(status="interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                     error=f"{type(error).__name__}: {error}")
    finally:
        suite["finished_at"] = utc_now()
        write_json(summary_path, suite)
    print(f"Suite {suite['status']}; evidence: {summary_path}", flush=True)
    return 0 if suite["status"] == "success" else 1


def main():
    args = parser().parse_args()
    try:
        return execute_suite(args)
    except (OSError, ValueError, SyntaxError) as error:
        print(f"Cannot start timing suite: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
