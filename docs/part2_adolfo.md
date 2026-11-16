# Adolfo's Part 2 report material

Status: movement integration implemented locally; server Spark fixtures, smoke,
and real measurements pending. PR #5 remains a draft.
Do not present fixture values or unrun procedures as results.

## Server preflight evidence supplied by Adolfo

The following observations come from Adolfo's pasted server terminal output,
not from a connection made by the local assistant. The transcript does not
include a collection timestamp; record that with the eventual benchmark logs.

| Check | Observed value |
|---|---|
| Host | `class-146.cs.ucr.edu` |
| Activated conda environment | `cs179` |
| Python | `3.11.17` |
| Spark | `4.2.0` |
| Scala | `2.13.18` |
| Java | OpenJDK `17.0.18-internal` |
| MySQL server (via socket and TCP) | `5.7.20`, port `3307` |
| Cleaned input directories | `kaggle_tracking`, `kaggle_plays`, `kaggle_injuries` exist |
| Database tables | All five expected Part 2 tables exist |
| Existing timing rows | `0` at preflight; not a permanent assertion |
| Filesystem free space | `50G` available of `194G`, `75%` used at preflight |
| Shared checkout | `part2-moses` at `8e65c5a`, tracking `origin/part2-moses` |
| Shared untracked file | `part1_collection/clean_output.txt`; preserve it |
| Server Part 2 file listing | `01_injury_rates.py`, `common.py`, `schema.sql`; no movement job shown |

The live `timing_results` definition matches the inspected schema: auto-generated
`run_id INT` and `run_at TIMESTAMP`, required `workers INT`, `data_fraction DOUBLE`,
`seconds DOUBLE`, and nullable `rows_processed BIGINT`. Engine: InnoDB.

TCP access through the MySQL client succeeds at `127.0.0.1:3307`. This verifies
the endpoint, but not Spark/JDBC loading, authentication, or writes. Still
unverified: JDBC access from Spark, input file readability and contents, the
integrated movement entry point on the server, and real benchmark behavior. Directory existence alone
does not establish usable input data. Available disk space must be checked again
before a run; no capacity estimate has yet been made for optional physical subsets.

Preserve the shared checkout on Moses's branch and leave its untracked log intact.
Use the separate-worktree procedure in `part2_spark/TIMING.md` after the reviewed
Adolfo branch has been pushed. Neither local commit `558fc6c` nor its source files
are implied to be present in the shared server checkout.

## Implementation entry (draft)

`run_timing.py` launches the movement-feature benchmark sequentially for four
deterministic, play-preserving fractions on local[1] and local[2].
`benchmark_movement.py` invokes Joshua's movement transformations through a
reviewed adapter, verifies the active Spark master, materializes isolated outputs,
validates them, and appends successful timing rows through the shared MySQL helper.
It leaves the team's final movement tables untouched. `plot_timing.py` validates
the evidence and exports the timing table and execution-time graph.

`movement_adapter.py` delegates to `02_movement_features.py:build_features`.
The normal script's explicit `main()` calls that same function and retains its
full-data preview/publish behavior. Only the full job caches play features; it
unpersists them and stops Spark even on failure. The benchmark does not cache,
start another session, reread tracking, or call the full job's publishing entry.
The transformation syntax tree was checked against Joshua's merged implementation:
calculations and filters match, excluding persistence moved to the full entry.

Local verification: 22 tests discovered, 20 passed; the real Spark fixture and
Matplotlib rendering test skipped because these packages are absent locally.
Both benchmark plans and Python compilation passed. Local mocks are correctness
checks, not server or performance evidence. The opt-in Spark fixture checks valid
and excluded event boundaries, metadata filtering, known feature values, column
types, output validation failures, and repeated calls without persisted frames.
Run it in the existing server environment before the smoke test.

Output validation permits legitimate exclusions. It checks unique, non-null
output keys, required metadata, output membership in the selected tracking keys,
and exact per-group agreement between `movement_summary.plays` and `play_features`.
Evidence records selected tracking rows, selected keys, retained output plays,
and total exclusions separately. Exclusions combine boundary/metadata effects;
they are not a per-reason audit. Joshua reported 255,329 full-data player-play
outputs; compare that only against a full run of the same dataset and investigate
differences. It is not a universal assertion or a timing observation.

Server smoke validation, final-table preservation evidence, and the exact pushed
commit used for measurements must still be recorded before treating this as a
validated implementation. Joshua's pending full-data MySQL publishing remains
his separate step and is not performed by benchmark runs.

## Timing evidence to insert after server runs

- Server hostname, date/time window, actual conda/Python/Spark/Java versions.
- Reviewed commit hash, source hashes, adapter and Joshua job names.
- Timing boundary: session startup excluded; reads, filtering, computation, and
  isolated Parquet output included; validation and timing-record JDBC insert excluded.
- `timing_table.md` and `timing_plot.png` from one complete validated suite.
- Actual tracking rows, selected keys, retained outputs and exclusions at each fraction; same subset fingerprints
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

## Contribution so far

Adolfo Magallanes implemented the timing runner, evidence validation and plotting
tools, merged Joshua's delivered movement job into the timing branch, and exposed
the original transformations through a shared import-safe function and real
benchmark adapter. Local correctness checks passed as described above. Server
execution, MySQL timing inserts, final graph/table and measured discussion remain
pending; no speedup has been established.

## Contribution text (use only after completion)

Adolfo Magallanes implemented and tested the timing runner and plotting tools,
integrated Joshua's movement-feature code for benchmarking, ran the eight required
configurations on class-146, appended validated timings to MySQL, and prepared
the timing table, graph, evidence, and scaling discussion.

The preceding text is a completion template, not a statement of work already run.
Current completed work must be described separately from pending validation and
server execution. Moses owns infrastructure/H1, Joshua owns movement calculations,
and Colin owns the nflverse analysis.
