# Historical Spark analysis and reconciliation (C12)

`quality_monitor.history` queries the clean Parquet archive independently of the streaming aggregation. It produces per-station/event-time trends grouped by run, category and model/threshold version, then compares their full window identities, counts and reject rates with finalized SQLite rows. It reads the Q3 checkpoint's `quality-store.json` for the actual window size, input path and database instead of assuming matching settings.

The command is read-only with respect to runtime state. It writes one new JSON report and does not advance watermarks, publish controls, change checkpoints or repair serving results. It collects only explicitly bounded aggregate windows (1,000 by default), never the full inspection history. Raw/clean ID counts and joins run in Spark.

## Reproduce the controlled check

Run the existing explicitly invoked pipeline scenario:

```bash
docker compose run --build --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-windows --master 'local[2]' --conf spark.ui.enabled=false /app/scripts/check_pipeline.py
```

This scenario still uses Q1's real validation on Kafka-shaped rows without a broker. It now writes the final Q3 windows through the real SQLite sink, preserving a versioned query contract, then invokes the historical comparison with the independent fixture oracle. It retains a fresh `/data/c06-check-<id>` folder containing raw/clean Parquet, Q2/Q3 checkpoints, `serving/quality.sqlite`, `result.json` and `history.json`. The prefix is retained for compatibility with earlier verification records. Current checks no longer write a separate `final/` Parquet window sink.

It verifies both deliberately delayed inspections before closure, then injects a distinct inspection after the watermark horizon. An extra changed-count comparison confirms that reconciliation detects an incorrect serving result. This extends the existing scenario; there is no new test suite, model dependency or default integration run.

## Run the standalone query

Use the actual path printed by the check. Choose a new report name if rerunning:

```bash
CHECK_ROOT='/data/c06-check-<id>'
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-windows --master 'local[2]' --conf spark.ui.enabled=false /app/quality_monitor/history.py --checkpoint "$CHECK_ROOT/q3" --raw "$CHECK_ROOT/raw" --run-id fixture-001 --expected /app/tests/fixtures/expected_counts.json --output "$CHECK_ROOT/history-cli.json"
```

`--raw` must refer to the matching raw archive. The clean input, database and window size come from the supplied Q3 contract. A non-final checkpoint is rejected: the earlier real bottle smoke produced provisional rows and cannot be called a closed-run reconciliation. Stop/drain a controlled scenario and advance event time through supported closure before checking final results; a single station stop does not do that.

The optional `--expected` file uses the same per-station `inspections`, `rejects` and `reject_rate` format as `tests/fixtures/expected_counts.json`. Supply an independent expectation for the actual scenario, not the fixture oracle for unrelated model runs. Without it, a match checks archive-versus-serving consistency only; the report explicitly says no oracle was supplied.

Use `--max-windows` to set an explicit aggregate-result bound. The command fails rather than silently truncating a large run. The report contains bounded historical window rows suitable for plotting trends, station totals, reconciliation differences and accounting summaries; no new dashboard service is required.

## What is counted

| Report field | Meaning |
| --- | --- |
| `raw_archive_global` | Total/invalid rows across the supplied raw path. Malformed JSON without a run ID cannot honestly be assigned to a particular run. |
| `raw_run.valid_inspection_rows` / `unique_inspection_ids` | Valid inspection messages and distinct logical identities for the selected run. Their difference is repeated inspection rows. |
| `raw_run.unique_raw_ids_absent_from_clean` | Distinct valid raw inspection IDs not retained in clean history. This is an exclusion count, not an automatic diagnosis of lateness. |
| `clean_run` | Inspection/control counts, repeated IDs and clean inspection IDs lacking raw support. Controls never enter quality totals. |
| `historical_windows` / `station_totals` | Batch time trends and weighted station totals, keeping detector groups distinct in the window rows. Raw anomaly scores are not pooled across categories. |
| `window_mismatches` / `provisional_serving_windows` | Missing/extra/different windows and non-final serving rows. Either prevents a successful reconciliation. |
| `oracle_supplied` / `oracle_matches` | Whether independent expected station totals were checked and whether they matched. |

A successful result requires nonempty final serving rows, matching full window keys/counts/rates, no repeated clean IDs or unsupported clean inspections, and a matching oracle if provided. The CLI returns a nonzero status for a comparison failure and keeps the report for investigation. Missing state and query-contract errors also fail explicitly. Reports and runtime data belong outside Git; commit only small observed summaries.

## Recorded result — 14 November 2026

The extended scenario and standalone CLI passed on Spark 4.0.2 with `local[2]`, two shuffle partitions, one-minute windows and two-minute lateness. Batch queries, final SQLite windows and the independent expected result all gave A=60 inspections/6 predicted rejects (10%) and B=60/12 (20%).

The raw archive contained 137 rows: 131 valid inspection messages, four controls and two invalid payloads. Among valid inspections there were 121 distinct IDs: 120 supported IDs plus one deliberately beyond-horizon ID. Ten rows were repeats. Clean history contained 120 inspections and four controls, with no repeated IDs or inspections unsupported by raw data. The known late ID was absent from clean history; both within-horizon delayed inspections were accepted. Two final windows matched with no differences. One invalid payload could be attributed to the run and one malformed payload had no run ID.

See [the small observed record](results/historical-reconciliation.json). This is correctness evidence on a controlled fixture, not a history-volume benchmark, new Kafka transport check or processor-crash recovery result. A batch query does not reconstruct arrival order or Spark's watermark decisions. If clean history includes inspections that Q3 legitimately excluded, the mismatch must be explained with the actual supported event set; do not silently remove records to force a match. C13 recovery, C14 capacity and the complete actual-image demo remain separate work.
