# Adolfo's measured timing evidence

`measured-20261116T045249Z/` is the complete required suite from class-146,
suite `c02d1a57-1df9-41b3-b867-e4a5694b7499`. The measured code is exactly
`fd9d641f14e7ff5a0b47e05d1de93a40c2a4dd11`. Later commits document and verify
these observations; they are not the measured code version.

The 34 original evidence files were copied byte-for-byte from the locally
transferred `timing_runs/measured-20261116T045249Z/`: summary, MySQL export,
and each run's command, result, startup marker and log. They contain the real
server paths, times, selected-input fingerprints, source hashes, settings and
append outcomes. Logs retain the incubator-vector and pandas compatibility
warnings; all eight processes exited successfully. No credential values were
found in the retained evidence review.

`verification.json` reconciles all eight appended measurements to unique exported
MySQL rows 1-8 and their append windows, and verifies the nine Python source hashes
against Git blobs at the measured commit. The SQL timestamps contain no timezone;
interpreting them as PST (UTC-08:00) places every insert within its recorded
validation-to-completion window. It also checks commands, log summaries, output
schemas, count arithmetic, sequential ordering and matching subsets across
masters through the existing validator. It does not contact the server or
recompute transformations from the raw data. Fingerprints are consistency checks,
not cryptographic proofs that input rows are identical.

`report/` contains the table, PNG/SVG graph, plot-source metadata and exact local
plotting package versions. They were produced with the existing `plot_timing.py`,
without fixture or partial-result options. The PNG was visually inspected:
complete axes, readable legend, eight points, and labels for actual selected
tracking rows and local execution threads. `SHA256SUMS` covers all files in the
suite directory except itself; it detects later changes to archived artifacts.
Scoped Git attributes preserve exact archive bytes on Windows and Linux, including
the CSV's original line endings and Matplotlib's native SVG path whitespace.

The final analysis tables are empty in the SQL export. The isolated Parquet
feature outputs are distinct from those tables. Joshua's full-data MySQL
publishing remains a separate team task.

Not committed: generated Parquet outputs, caches, virtual environments, the
redundant transfer archive, fixture previews, or smoke artifacts. The report
keeps the earlier smoke and synthetic tests separate from the measured suite.

## Recheck locally without Spark or MySQL

From the repository root, ordinary Python 3.11+ can reconcile the archive:

```powershell
python docs/evidence/part2-adolfo/verify_evidence.py docs/evidence/part2-adolfo/measured-20261116T045249Z
```

To reproduce plots using the already prepared local virtual environment, choose
a new output directory (the plotting tool refuses to overwrite one):

```powershell
$env:MPLCONFIGDIR = Join-Path (Get-Location) 'timing_runs/.mplconfig'
& './timing_runs/.venv-plot/Scripts/python.exe' part2_spark/plot_timing.py --results docs/evidence/part2-adolfo/measured-20261116T045249Z/summary.json --output-dir timing_runs/measured-replot
```

If recreating that environment on this Windows machine:

```powershell
& 'C:/Users/adolf/miniconda3/python.exe' -m venv timing_runs/.venv-plot
& './timing_runs/.venv-plot/Scripts/python.exe' -m pip install -r part2_spark/requirements-timing.txt
```

No Spark or MySQL installation is needed for reconciliation or plotting.

Local checks for this handoff: 22 tests ran, 21 passed including fixture-plot
rendering, and the opt-in Spark test skipped. Adolfo separately reported the
server's 22-test result with only the Matplotlib test skipped. Those correctness
tests are not benchmark trials and their fixtures are not part of this archive.
