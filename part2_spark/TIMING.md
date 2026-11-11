# Adolfo's Part 2 timing work

Due November 18, 2026. Owner: Adolfo Magallanes (GorillaAdolfo).

This implementation supplies timing orchestration, evidence validation, and plotting.
It does not implement Joshua's movement features. At the initial inspection,
`origin/main` was `76a464a`, Moses's PR #4 was merged, and no movement job was
available in the fetched branches or open pull requests. The adapter template is
intentionally disabled. No real performance results exist yet.

## Routine sanity check

At implementation milestones and before a server run, check:

- Work stays on `part2-adolfo` and within timing experiments; no stadium cleanup,
  injury-rate changes, shared infrastructure changes, or replacement movement logic.
- Existing work is preserved. Review `git status --short` and the intended diff.
- `common.py`, `schema.sql`, teammates' jobs, and final analysis tables stay untouched.
- Synthetic fixtures are labeled; real measurements come only from class-146.
- The adapter uses Joshua's actual calculation code and has no startup/actions/writes
  at import time. It consumes the supplied subset without rereading full tracking data.
- Benchmark helper writes target only `timing_results` with explicit append mode.
- No commit/push until Adolfo requests it. Use command-scoped author AND committer
  identity, a message under 10 words, intended files only, no attribution trailers,
  and no force-push. Verify staged diff and commit metadata before any later push.

## Local Windows development

PowerShell, from this checkout:

```powershell
Set-Location 'D:\CS179G Project\Pivot'
python -m unittest discover -s part2_spark/tests -v
python part2_spark/run_timing.py --plan
python part2_spark/run_timing.py --plan --smoke
```

Planning and logic tests need only Python 3.11 and the standard library. They do
not connect to Spark, MySQL, or the server. The rendering test skips if Matplotlib
is unavailable. Install only `requirements-timing.txt` in a separate plotting
environment if necessary; do not install Spark or MySQL locally. The Windows
MSYS Python may not accept regular Windows binary plotting wheels; use an existing
ordinary CPython/conda plotting environment for that optional dependency.

Explicit synthetic plotting check, using a Python environment with Matplotlib:

```powershell
python part2_spark/plot_timing.py --results part2_spark/tests/fixtures/timing_results.json --output-dir timing_runs/fixture-preview --allow-fixtures
```

The preview title and Markdown table say `FIXTURE ONLY - not performance
measurements`. It is not report evidence. Output directories must be new;
commands deliberately refuse to overwrite prior runs or plots. `timing_runs/`
is ignored by Git, as are CSV files under the existing repository rules.

## Integration dependency: Joshua's movement code

Do not guess a script filename or CLI. Inspect his delivered implementation first.
The following is **our proposed adapter contract**, not a claim about his interface:

```python
BENCHMARK_ADAPTER_VERSION = 1
BENCHMARK_ADAPTER_READY = True  # only after integration and review

def build_features(*, spark, tracking, plays, injuries):
    # Call Joshua's actual import-safe transformation functions here.
    # Return two lazy DataFrames; no new movement formulas, actions, or writes.
    return {"play_features": features, "movement_summary": summary}
```

Start from `movement_adapter_template.py`, which has readiness set to False and
raises an error. The ready adapter must live in the same reviewed checkout and
return exactly the columns in `schema.sql`. If his job is currently a top-level
script with unconditional writes, coordinate an import-safe transformation entry
point and a no-final-writes path with Joshua. Do not run or import that script
until its side effects have been inspected. Keep the actual formulas under his
ownership. Adapter imports must work when `spark-submit` runs this driver; use
the existing `part2_spark` module path or explicitly import reviewed code by path.

The driver guards `common.write_table` **before importing the adapter**, blocking
all adapter calls to it. Its own saved helper appends one validated timing row.
This guard is not a sandbox: direct JDBC/file writes or another Spark session in
an adapter would bypass it, so code review remains necessary. Do not mark an
adapter ready until those behaviors are absent.

## Measurement definition

Required: eight fresh, sequential `spark-submit` processes, in fraction order
25%, 50%, 75%, 100%, running local[1] then local[2] at each fraction.
The actual master is checked before reads, after adapter construction, and after
materialization. No process runs two benchmark configurations concurrently.

`local[1]` and `local[2]` mean one and two local execution threads, not separate
worker processes or a multi-machine cluster. Ask the instructor/TA whether this
matches the rubric's word "workers"; the shared SQL column retains that name.
Reference: <https://spark.apache.org/docs/latest/submitting-applications.html>.

Sampling uses `pmod(xxhash64(seed, cast(PlayKey as string)), 1000000)` and nested
thresholds. All rows for a selected PlayKey are retained. Fractions describe hash
buckets of play keys, not exact percentages of rows. The default seed is 179;
both masters use the same seed. The full-data case includes every row. Null
PlayKey values invalidate the run. Record actual selected tracking rows and
distinct plays, not the checklist's approximate 76M figure multiplied by a fraction.

The monotonic timer starts **after Spark session startup**, immediately before
reading the three cleaned Parquet datasets. It includes input reads, selection,
Joshua's feature/summary transformations, and writing both complete outputs as
Parquet in the run's unique `outputs/` directory. These writes force evaluation
of every output column. There is no pre-count or tracking persistence before the
timer. The driver adds no Spark caching; any caching introduced by the actual
adapter must be reviewed and documented before integration. If the two output
plans recompute common work, that time is included consistently.

Excluded: Python/JVM/JDBC-package startup, session creation (recorded separately),
input file metadata collection, validation/counting/fingerprinting, and appending
the timing record. This measures computation plus isolated Parquet output, **not
the final analysis-table JDBC write time**. Validation outside the timer reads the
selected tracking again and validates saved output counts and primary keys.
The play-feature output must cover exactly the selected PlayKeys, with one row
per key and no plays outside the subset. Missing plays fail validation rather than
silently reporting a different workload; discuss such failures with Joshua.
Spark actions trigger execution; merely constructing lazy transformations is not
a completed measurement. Reference:
<https://spark.apache.org/docs/latest/rdd-programming-guide.html>.

Important limitations for the report:

- Hash filtering may still scan the complete tracking Parquet input at each
  fraction. This is scaling by selected rows, not guaranteed reduced input I/O.
  If the rubric requires smaller physical input datasets, prepare immutable,
  play-preserving subsets separately after checking disk space; exclude subset
  preparation time and keep layouts fixed between masters. That staging path is
  not implemented in this first version.
- Input metadata (names, sizes, mtimes) is hashed before/after a run. Selected-row
  counts, distinct plays, and a sum of full-row xxhash64 values are compared between
  masters before the second measurement is appended. The row fingerprint is an
  order-independent consistency check, not a cryptographic proof of equality.
- Validation and previous runs can warm OS caches. Do not clear shared caches.
  local[2] follows local[1], so discuss order effects and shared-server load.
  Optional later repetitions can assess variability; the eight required runs
  remain distinct from repetitions. Do not claim statistical certainty from one run.
- Both masters must use the same source hashes, input metadata, seed, host, Spark
  version, and recorded shuffle/adaptive settings. Default parallelism differs
  intentionally. Record external load and JVM/memory settings in your run notes.

## Server Linux: preflight (Adolfo runs these)

Do not install Codex on the server. These are commands for your existing Linux
terminal on the expected host, not commands executed by the local assistant.
They inspect state; they do not start MySQL, recreate tables, or alter shared data.

```bash
hostname -f
conda activate cs179
python --version
spark-submit --version
java -version
git -C /home/cs179g/Pivot status --short --branch
git -C /home/cs179g/Pivot remote -v
ls -ld /home/cs179g/nfl_data/clean/{kaggle_tracking,kaggle_plays,kaggle_injuries}
df -h /home/cs179g
mysql --socket="$HOME/mysql/mysql.sock" -u root -e 'SELECT VERSION(), @@port; SHOW TABLES FROM pivot; SHOW CREATE TABLE pivot.timing_results;'
mysql -h 127.0.0.1 -P 3307 -u root pivot -e 'SELECT 1; SELECT COUNT(*) FROM play_features; SELECT COUNT(*) FROM movement_summary; SELECT COUNT(*) FROM timing_results;'
```

Verify the host, conda environment, data readability, port/schema, and output
storage before proceeding. If MySQL is unavailable or schema differs, stop and
report it to Moses; do not restart or modify shared infrastructure here. Credentials
should come from the agreed environment, never CLI password arguments, committed
files, or copied logs. Do not dump environment variables. Review logs for secrets
before sharing them. The driver's own errors redact MYSQL_PASSWORD and MYSQL_URL;
Spark/JDBC subprocess output cannot be guaranteed secret-free.

After **Adolfo requests a commit/push and the reviewed branch is pushed**, create
a separate worktree; do not switch the team's shared checkout:

```bash
git -C /home/cs179g/Pivot fetch origin
git -C /home/cs179g/Pivot worktree list
git -C /home/cs179g/Pivot worktree add --detach /home/cs179g/Pivot-adolfo origin/part2-adolfo
cd /home/cs179g/Pivot-adolfo
git status --short --branch
git rev-parse HEAD
```

Use a new worktree path if that path already exists. Do not remove/reset an existing
worktree. This detached worktree pins the fetched branch version for testing. Keep
the source, adapter, and cleaned inputs unchanged throughout a suite. Arrange a
quiet testing window with teammates rather than stopping their jobs.

## Server Linux: smoke, then required matrix

**Blocked until Joshua's real adapter is ready and reviewed.** The placeholder
below must be replaced by that verified adapter path; it is not his job filename.

```bash
cd /home/cs179g/Pivot-adolfo
conda activate cs179
adapter=/absolute/path/to/reviewed_movement_adapter.py
python part2_spark/run_timing.py --plan
smoke_dir="$HOME/pivot_timing/smoke-$(date -u +%Y%m%dT%H%M%SZ)"
python part2_spark/run_timing.py --adapter "$adapter" --data-root /home/cs179g/nfl_data/clean --output-dir "$smoke_dir" --smoke
```

The smoke test selects about 1% of play keys on local[1], validates isolated outputs,
and **does not insert into timing_results**. Review its `summary.json`, `run.log`,
row counts, and actual output columns before launching the required work:

```bash
matrix_dir="$HOME/pivot_timing/required-$(date -u +%Y%m%dT%H%M%SZ)"
python part2_spark/run_timing.py --adapter "$adapter" --data-root /home/cs179g/nfl_data/clean --output-dir "$matrix_dir"
```

Every child command includes `--master 'local[1]'` or `--master 'local[2]'` and
`--packages com.mysql:mysql-connector-j:8.4.0`. Commands are saved as exact argv
arrays in `command.json`; both stdout and stderr go to `run.log`. The runner stops
at the first failure by default, preserving evidence. `--continue-on-error` is an
explicit diagnostic option; there is no automatic retry/resume. Store any later
attempt in a new directory and keep failed attempts as evidence.

Successful required measurements use explicit Spark IntegerType/DoubleType/
LongType/DoubleType fields and call the original shared helper with
`write_table(df, "timing_results", mode="append")`. MySQL fills `run_id`/`run_at`.
No analysis tables are written by the driver.

Before JDBC, `result.json` records `append_pending`; a failed JDBC call records
`append_uncertain`. An interrupted process may leave `append_pending`. A JDBC
failure can occur after MySQL committed the row, and Spark task retries can also
produce duplicates: the current table has no unique experiment ID. **Do not retry
these inserts blindly.** Compare the recorded time window and values with MySQL
and ask Moses before any correction; no automatic deletion/deduplication occurs.
If the launcher reports failure after a successful worker result, inspect the
worker result and MySQL before retrying as well.

## Tables, graph, and report evidence

With Matplotlib available, run on the server or copy the small summary JSON to a
local plotting environment (no tracking data needed). For server plotting, first
check `python -c 'import matplotlib; print(matplotlib.__version__)'`. If absent,
prefer a separate local plotting environment; do not change the shared conda
environment just to make the graph.

```bash
python part2_spark/plot_timing.py --results "$matrix_dir/summary.json" --output-dir "$matrix_dir/report"
mysql -h 127.0.0.1 -P 3307 -u root pivot -e 'SELECT run_id, workers, data_fraction, rows_processed, seconds, run_at FROM timing_results ORDER BY run_id;'
mysql -h 127.0.0.1 -P 3307 -u root pivot -e 'SELECT COUNT(*) FROM play_features; SELECT COUNT(*) FROM movement_summary;'
```

For a strong before/after check that final tables stayed unchanged, capture
`CHECKSUM TABLE pivot.play_features, pivot.movement_summary` before and after,
only if acceptable to teammates (it may scan tables and affect cache/load).
Perform that outside measurements. Row counts alone cannot prove unchanged contents.

Outputs: `timing_table.md`, `timing_table.csv`, `timing_plot.png`, `timing_plot.svg`,
and `plot_source.json`. Normal plotting requires all eight successful, appended,
matching measurements. `--allow-partial` produces an explicitly incomplete
diagnostic graph. It cannot turn smoke tests into report measurements. Fixture
and real data cannot be mixed, and repetitions are not silently averaged.

Keep the whole suite directory, including failed logs and the source hashes.
Capture a timing terminal screenshot, the scoped MySQL rows, and the graph.
Use the experiment time window and logged values to identify your rows in the
shared table; it may contain other people's runs. Do not claim the table has
exactly eight rows globally. Fill `docs/part2_adolfo.md` from this real evidence.
