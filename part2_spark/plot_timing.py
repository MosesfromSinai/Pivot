"""Produce a report table and static plots from validated timing evidence."""

import argparse
import csv
import math
from pathlib import Path

from timing_support import (
    FORMAT_VERSION, FRACTIONS, WORKERS, ensure_paired_subsets, read_json, write_json,
)


def load_measurements(path, allow_fixtures=False, allow_partial=False):
    suite = read_json(path)
    if suite.get("format_version") != FORMAT_VERSION:
        raise ValueError("Unsupported timing evidence format.")
    if type(suite.get("fixture")) is not bool:
        raise ValueError("Evidence must explicitly identify whether it is a fixture.")
    if suite.get("kind") != "required":
        raise ValueError("Smoke tests are not report measurements.")
    fixture = suite.get("fixture") is True
    if fixture and not allow_fixtures:
        raise ValueError("Fixture evidence requires --allow-fixtures and is never a benchmark result.")
    if not allow_partial and suite.get("status") != "success":
        raise ValueError("Suite is incomplete or failed; use --allow-partial for a labeled diagnostic plot.")
    records = []
    seen = set()
    for record in suite.get("runs", []):
        if record.get("fixture") is not fixture:
            raise ValueError("Mixed fixture and real evidence is not allowed.")
        if record.get("status") != "success":
            continue
        if record.get("suite_id") != suite.get("suite_id"):
            raise ValueError("Run belongs to a different timing suite.")
        workers, fraction = record.get("workers"), record.get("data_fraction")
        if workers not in WORKERS or fraction not in FRACTIONS:
            raise ValueError("Unexpected worker count or fraction in report evidence.")
        if record.get("master") != f"local[{workers}]":
            raise ValueError("Actual Spark master does not match the reported thread count.")
        if not fixture and record.get("appended") is not True:
            raise ValueError("Real report measurements must have been appended to timing_results.")
        rows, seconds = record.get("rows_processed"), record.get("seconds")
        if type(rows) is not int or rows <= 0 or not isinstance(seconds, (int, float)):
            raise ValueError("Missing or invalid measurement values.")
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("Execution time must be finite and positive.")
        key = (workers, fraction)
        if key in seen:
            raise ValueError("Duplicate configuration; select one suite instead of silently averaging runs.")
        seen.add(key)
        records.append(record)
    expected = {(w, f) for w in WORKERS for f in FRACTIONS}
    complete = seen == expected
    if not complete and not allow_partial:
        raise ValueError("All eight successful configurations are required for the report.")
    if not records:
        raise ValueError("No successful timing measurements found.")
    ensure_paired_subsets(records)
    # A missing fingerprint cannot be used to demonstrate identical subsets.
    if not fixture and any(not r.get("subset_fingerprint") or not r.get("input_manifest")
                           or not r.get("code", {}).get("source_sha256") for r in records):
        raise ValueError("Real evidence is missing input/code provenance or subset fingerprints.")
    return suite, records, complete


def table_rows(records):
    indexed = {(r["workers"], r["data_fraction"]): r for r in records}
    rows = []
    for fraction in FRACTIONS:
        pair = [indexed.get((w, fraction)) for w in WORKERS]
        if not any(pair):
            continue
        count = next(r["rows_processed"] for r in pair if r)
        one, two = [r["seconds"] if r else None for r in pair]
        rows.append({"data_fraction": fraction, "rows_processed": count,
                     "local1_seconds": one, "local2_seconds": two,
                     "speedup_1_over_2": one / two if one is not None and two is not None else None})
    return rows


def write_table(output_dir, rows, label):
    with (output_dir / "timing_table.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = [label, "", "| Fraction | Actual tracking rows | local[1] (s) | local[2] (s) | Speedup |",
             "|---|---:|---:|---:|---:|"]
    def value(number):
        return f"{number:.3f}" if number is not None else "missing"
    for row in rows:
        lines.append(
            f"| {row['data_fraction']:.0%} | {row['rows_processed']:,} | "
            f"{value(row['local1_seconds'])} | {value(row['local2_seconds'])} | "
            f"{value(row['speedup_1_over_2'])} |"
        )
    (output_dir / "timing_table.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_plot(output_dir, records, label):
    try:
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg
    except ImportError as error:
        raise RuntimeError(
            "Plotting requires matplotlib. Install requirements-timing.txt in a plotting "
            "environment; Spark and MySQL are not needed for plotting."
        ) from error
    figure = Figure(figsize=(8.5, 5.2), layout="constrained")
    FigureCanvasAgg(figure)
    axes = figure.subplots()
    for workers, color, marker in ((1, "#176b4a", "o"), (2, "#3459a3", "s")):
        points = sorted((r for r in records if r["workers"] == workers),
                        key=lambda r: r["rows_processed"])
        axes.plot([r["rows_processed"] for r in points], [r["seconds"] for r in points],
                  marker=marker, color=color, linewidth=2, label=f"local[{workers}] ({workers} local thread{'s' if workers > 1 else ''})")
    axes.set(title=label, xlabel="Actual tracking rows selected", ylabel="Execution time (seconds)")
    axes.set_ylim(bottom=0)
    axes.set_xlim(left=0)
    axes.ticklabel_format(axis="x", style="sci", scilimits=(6, 6), useMathText=True)
    axes.grid(alpha=0.2)
    axes.legend(loc="best")
    figure.savefig(output_dir / "timing_plot.png", dpi=180)
    figure.savefig(output_dir / "timing_plot.svg")


def export_results(source, output_dir, allow_fixtures=False, allow_partial=False):
    suite, records, complete = load_measurements(source, allow_fixtures, allow_partial)
    label = "Movement feature execution time"
    if suite["fixture"]:
        label = "FIXTURE ONLY - not performance measurements"
    if not complete:
        label += " (INCOMPLETE)"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    # Render first so a missing plotting dependency cannot leave plausible final tables.
    render_plot(output_dir, records, label)
    write_table(output_dir, table_rows(records), label)
    write_json(output_dir / "plot_source.json", {
        "source": str(Path(source).resolve()), "suite_id": suite["suite_id"],
        "fixture": suite["fixture"], "complete": complete, "label": label,
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path, help="Suite summary.json.")
    parser.add_argument("--output-dir", required=True, type=Path, help="New output directory.")
    parser.add_argument("--allow-fixtures", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    try:
        export_results(args.results, args.output_dir, args.allow_fixtures, args.allow_partial)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(2, f"Cannot plot timing evidence: {error}\n")
    print(f"Table and plots written to {args.output_dir}")


if __name__ == "__main__":
    main()
