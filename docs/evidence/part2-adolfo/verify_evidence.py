"""Reconcile the archived measured suite, pinned Git sources, logs and MySQL export.

Postprocessing only: no Spark session, database connection or server access.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "part2_spark"))
from plot_timing import load_measurements, table_rows
from timing_support import OUTPUT_COLUMNS, configurations, read_json


def verify(root):
    manifest = root / "SHA256SUMS"
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            digest, name = line.split("  ", 1)
            path = (root / name).resolve()
            assert path.is_relative_to(root.resolve())
            assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, name
    suite, runs, complete = load_measurements(root / "summary.json")
    assert complete and len(runs) == 8 and suite["planned"] == configurations()
    assert [r["run_id"] for r in runs] == [c["run_id"] for c in configurations()]
    measured_commit = "fd9d641f14e7ff5a0b47e05d1de93a40c2a4dd11"
    assert suite["code"]["git_commit"] == measured_commit
    assert suite["code"]["git_status"] == ""
    for server_path, expected in suite["code"]["source_sha256"].items():
        path = "part2_spark/" + server_path.rsplit("/", 1)[1]
        blob = subprocess.check_output(["git", "show", f"{measured_commit}:{path}"], cwd=REPO)
        assert hashlib.sha256(blob).hexdigest() == expected, path

    lines = [line for line in (root / "mysql-verification.txt").read_text().splitlines() if line]
    header = lines[0].split("\t")
    assert header == ["run_id", "workers", "data_fraction", "rows_processed", "seconds", "run_at"]
    sql_end = lines.index("play_features_rows")
    sql_rows = [dict(zip(header, line.split("\t"), strict=True)) for line in lines[1:sql_end]]
    assert len(sql_rows) == 8 and len({r["run_id"] for r in sql_rows}) == 8
    assert lines[sql_end:] == ["play_features_rows", "0", "movement_summary_rows", "0"]
    matches, previous_finish = [], suite["started_at"]
    sql_timezone = timezone(timedelta(hours=-8))  # November 15, 2026, PST.
    expected_types = {
        "play_features": ("string", "integer", "string", "string", "string", "double",
                          "double", "double", "double", "double", "integer", "boolean"),
        "movement_summary": ("string", "string", "integer", "double", "double", "double"),
    }
    for run in runs:
        run_id = run["run_id"]
        directory = root / run_id
        worker = read_json(directory / "result.json")
        assert run == {**worker, "exit_code": 0}, run_id
        assert run["phase"] == "complete" and run["kind"] == "required"
        assert run["code"] == suite["code"]
        assert run["output_rows"]["play_features"] == run["retained_output_plays"] > 0
        assert run["retained_output_plays"] + run["excluded_plays"] == run["selected_plays"]
        assert run["excluded_plays"] >= 0
        assert read_json(directory / "worker_started.json")["started_at"] == run["started_at"]
        command = read_json(directory / "command.json")
        argv = command["argv"]
        for flag, expected in (("--master", run["master"]), ("--workers", str(run["workers"])),
                               ("--fraction", str(run["data_fraction"])), ("--seed", str(run["seed"])),
                               ("--suite-id", suite["suite_id"]), ("--run-id", run_id),
                               ("--packages", "com.mysql:mysql-connector-j:8.4.0")):
            assert argv[argv.index(flag) + 1] == expected, (run_id, flag)
        assert "--record-timing" in argv
        assert previous_finish <= command["started_at"] <= run["started_at"]
        previous_finish = run["finished_at"]
        for name, fields in expected_types.items():
            schema = run["output_schemas"][name]["fields"]
            assert tuple(f["name"] for f in schema) == OUTPUT_COLUMNS[name]
            assert tuple(f["type"] for f in schema) == fields
        footers = [json.loads(line) for line in (directory / "run.log").read_text().splitlines()
                   if line.startswith('{"run_id":')]
        assert len(footers) == 1
        assert footers[0] == {key: run[key] for key in footers[0]}, run_id
        sql_matches = [row for row in sql_rows
                       if int(row["workers"]) == run["workers"]
                       and float(row["data_fraction"]) == run["data_fraction"]
                       and int(row["rows_processed"]) == run["rows_processed"]
                       and math.isclose(float(row["seconds"]), run["seconds"], rel_tol=1e-12, abs_tol=1e-9)]
        assert len(sql_matches) == 1, run_id
        sql = sql_matches[0]
        inserted = datetime.strptime(sql["run_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=sql_timezone)
        assert datetime.fromisoformat(run["validated_at"]) <= inserted <= datetime.fromisoformat(run["finished_at"])
        matches.append({"run_id": run_id, "mysql_run_id": int(sql["run_id"]),
                        "mysql_run_at": sql["run_at"], "seconds": run["seconds"]})
    assert len({m["mysql_run_id"] for m in matches}) == 8
    assert previous_finish <= suite["finished_at"]
    full = [r for r in runs if r["data_fraction"] == 1.0]
    assert full[0]["retained_output_plays"] == full[1]["retained_output_plays"]
    return {
        "status": "verified", "suite_id": suite["suite_id"], "measured_commit": measured_commit,
        "successful_appended_runs": len(runs), "source_files_verified": len(suite["code"]["source_sha256"]),
        "paired_subsets_and_counts_match": True, "sequential_order_verified": True,
        "per_run_records_commands_schemas_and_log_footers_match": True,
        "mysql_timestamp_interpretation": "PST (UTC-08:00); consistent with every run's append window",
        "mysql_matches": matches, "final_table_counts": {"play_features": 0, "movement_summary": 0},
        "full_data_retained_plays": full[0]["retained_output_plays"], "timing_table": table_rows(runs),
        "limits": ["Reconciles archived evidence; does not query the server or reread raw Parquet outputs.",
                   "Subset fingerprints are consistency checks, not cryptographic proofs of row equality.",
                   "MySQL export has no experiment ID; rows are matched by values and append windows."],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.root)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
