# Adolfo's Part 2 report material

Status: movement integration and server smoke validated; server Spark fixture
results and the eight required measurements remain pending. PR #5 remains a draft.
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

At that earlier preflight, TCP client access succeeded at `127.0.0.1:3307`, but
Spark/JDBC loading, reads and the movement entry point were still unverified.
The later smoke evidence below establishes input reads and movement execution,
and JDBC package loading. JDBC authentication/inserts remain untested. Available
disk space must be checked again before the required matrix; optional physical
subsets have not been prepared or sized.

Preserve the shared checkout on Moses's branch and leave its untracked log intact.
Use the separate-worktree procedure in `part2_spark/TIMING.md` after the reviewed
Adolfo branch has been pushed. Neither local commit `558fc6c` nor its source files
are implied to be present in the shared server checkout.

## Server smoke evidence supplied by Adolfo

Adolfo reported that MySQL had stopped and the documented startup script was
missing. The existing MySQL 5.7.20 installation was restarted using its existing
data directory and resumed accepting TCP connections on port 3307. This recovery
was performed by the server users, not the local assistant. No restart commands
or changes to shared infrastructure were performed by the benchmark.

The supplied terminal transcript records a successful smoke suite at
`2026-11-16T04:47:39Z` through `04:48:51Z` (November 15, 2026, approximately
8:47-8:48 p.m. PST). Testing worktree: `/home/cs179g/Pivot-adolfo-timing`, clean
at `fd9d641f14e7ff5a0b47e05d1de93a40c2a4dd11`. Evidence directory:
`/home/cs179g/pivot_timing/smoke-20261116T044739Z`; suite ID:
`2ca90c13-e201-43e6-91b9-c4d8b47f80ba`.

| Smoke observation | Value |
|---|---|
| Host / Python / Spark | `class-146` / `3.11.17` / `4.2.0` |
| Master / fraction / seed | `local[1]` / `0.01` / `179` |
| Selected tracking rows | 778,421 |
| Selected PlayKeys | 2,772 |
| Retained play-feature outputs | 2,658 |
| Excluded selected plays | 114 (4.11%) |
| Movement summary groups | 20 |
| Measured smoke boundary | 45.238648456 seconds |
| Session startup (excluded) | 3.998954075 seconds |
| Shuffle partitions / adaptive execution | `200` / `true` |
| Status / process exit / appended | `success` / `0` / `false` |
| MySQL counts before and after | `play_features=0`, `movement_summary=0`, `timing_results=0` |

The local assistant parsed the pasted summary, checked its count arithmetic and
success metadata, and verified all nine recorded Python source SHA-256 values
against the pinned Git blobs. The worker reported successful saved-output checks
for primary keys, required fields, subset membership and summary group counts.
Recorded output columns/types match the intended movement schema. Spark schema
nullable flags for some required fields do not indicate observed nulls; the run's
validation checks actual values. The before/after queries show both final
movement tables empty at those two checkpoints; they are not continuous auditing.

The logs show MySQL Connector/J 8.4.0 loaded. Smoke mode did not call the timing
JDBC writer, so successful MySQL authentication/inserts from Spark remain pending.
The transcript does not include the opt-in synthetic Spark fixture test output;
run that check before the required matrix, or supply its earlier successful log.
The 45.24-second smoke observation is not one of the eight required measurements,
and does not establish speedup. The hash-filtered 1% subset may still scan full
input files. Keep this smoke directory intact and keep the server checkout pinned
through the required matrix; documentation-only follow-up commits need not be
fetched into the testing worktree.

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
Its server result is still pending in the supplied evidence; run it before the matrix.

Output validation permits legitimate exclusions. It checks unique, non-null
output keys, required metadata, output membership in the selected tracking keys,
and exact per-group agreement between `movement_summary.plays` and `play_features`.
Evidence records selected tracking rows, selected keys, retained output plays,
and total exclusions separately. Exclusions combine boundary/metadata effects;
they are not a per-reason audit. Joshua reported 255,329 full-data player-play
outputs; compare that only against a full run of the same dataset and investigate
differences. It is not a universal assertion or a timing observation.

The smoke validates the integrated path at the pinned commit recorded above.
Required-matrix validation, MySQL timing inserts, and matching subsets across
both masters remain pending. Joshua's pending full-data MySQL publishing remains
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
benchmark adapter. Local correctness checks passed as described above, and Adolfo
ran a successful server smoke with retained/excluded counts and unchanged empty
final tables. The eight required runs, MySQL timing inserts, final graph/table
and measured discussion remain pending; no speedup has been established.

## Contribution text (use only after completion)

Adolfo Magallanes implemented and tested the timing runner and plotting tools,
integrated Joshua's movement-feature code for benchmarking, ran the eight required
configurations on class-146, appended validated timings to MySQL, and prepared
the timing table, graph, evidence, and scaling discussion.

The preceding text is a completion template, not a statement of work already run.
Current completed work must be described separately from pending validation and
server execution. Moses owns infrastructure/H1, Joshua owns movement calculations,
and Colin owns the nflverse analysis.
