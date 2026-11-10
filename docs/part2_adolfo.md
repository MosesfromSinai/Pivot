# Adolfo's Part 2 report material

Status: implementation draft; Joshua integration and server measurements pending.
Do not present fixture values or unrun procedures as results.

## Implementation entry (draft)

`run_timing.py` launches the movement-feature benchmark sequentially for four
deterministic, play-preserving fractions on local[1] and local[2].
`benchmark_movement.py` invokes Joshua's movement transformations through a
reviewed adapter, verifies the active Spark master, materializes isolated outputs,
validates them, and appends successful timing rows through the shared MySQL helper.
It leaves the team's final movement tables untouched. `plot_timing.py` validates
the evidence and exports the timing table and execution-time graph.

**Before using this paragraph:** integrate the actual adapter and verify all of
these behaviors in the server smoke test. Record the actual adapter/job filename
and commit hash here. The adapter template is not a completed movement job.

## Timing evidence to insert after server runs

- Server hostname, date/time window, actual conda/Python/Spark/Java versions.
- Reviewed commit hash, source hashes, adapter and Joshua job names.
- Timing boundary: session startup excluded; reads, filtering, computation, and
  isolated Parquet output included; validation and timing-record JDBC insert excluded.
- `timing_table.md` and `timing_plot.png` from one complete validated suite.
- Actual tracking rows and distinct plays at each fraction; same subset fingerprints
  for both masters. Explain hash fractions versus actual row percentages.
- Commands, successful/failing run logs, MySQL evidence, screenshot, and final-table
  preservation check. Retain failures rather than counting them as measurements.

## Discussion outline (fill from observed measurements)

1. Describe how time changes across the four actual tracking-row counts for each
   master. Report seconds and the ratio T(local[1])/T(local[2]) at each size.
2. State whether two local threads helped, by how much, and where the effect varied.
   Do not call them separate worker processes. Confirm the rubric interpretation
   with the TA before claiming distributed-worker scaling.
3. Interpret observed behavior cautiously: full-input scans during hash filtering,
   Spark scheduling/shuffle overhead, output I/O, repeated computation, fixed job
   overhead, OS cache warming, and concurrent server activity are possible factors.
   Distinguish measured evidence from proposed explanations; do not assert an
   unmeasured CPU/I/O bottleneck as fact.
4. Explain that these timings include isolated Parquet output rather than production
   JDBC analysis writes, and exclude startup/validation. One run per configuration
   does not estimate variance. Label any additional repetitions separately.

## Contribution text (use only after completion)

Adolfo Magallanes implemented and tested the timing runner and plotting tools,
integrated Joshua's movement-feature code for benchmarking, ran the eight required
configurations on class-146, appended validated timings to MySQL, and prepared
the timing table, graph, evidence, and scaling discussion.

The preceding text is a completion template, not a statement of work already run.
Current completed work must be described separately from pending integration and
server execution. Moses owns infrastructure/H1, Joshua owns movement calculations,
and Colin owns the nflverse analysis.
