# Silver 06: reviewed service-date quarantine recovery

Status: the implementation prepared in commit
`16db48b5c6fbfe018f242e0d3b9ab5c1e31b01dc` has been deployed and validated on
Databricks through separately authorized controlled runs. Notebook source SHA-256:
`60e8504c1fa1dafe1a1b895803f0a5dd07e279f6f05c3d8cfcd1bce875014c20`.
The repository/workspace source comparison is byte-for-byte; this documentation
update does not change the notebook, exception registry, model or write gates.

## Verified cause and scope

Notebook 06 rebuilds Silver from pinned Bronze versions. Feed `1794498556`
(2026-11-12 15:49:16 UTC) has 33 invalid service dates in 25,366 rows: about
0.1301%, above the unchanged 0.1% per-snapshot gate. Rebuilding the history
repeats this failure even when later ingestion succeeds.

The exception covers exactly the reviewed identities of entity `1255`, trip
`101528146R50972`, vehicle `7740`, stop sequences 2–33. The isolated entity
`1308` row is not registered; it retains ordinary exclusion/gate treatment.
No date is invented and no Bronze row is changed.

Canonical identity fields, all represented as strings:
`feed_timestamp, entity_id, trip_id, stop_sequence, stop_id`.
Sort tuples lexicographically, join fields with `|`, join keys with LF without
trailing LF, then SHA-256 over UTF-8. Fingerprint:

`f1e98d9ab7629000f335e0d341c3e4b018bfbe46ba92af06aece03f97ea73867`

The checked content profile also requires empty raw start_date, route_id and
start_time, direction `0`, vehicle `7740`, and NULL service_date. NULL and empty
strings are deliberately distinct. A changed identity/profile is unreviewed,
not automatically quarantined; it must pass the original ordinary gate.
Ingestion timestamps and arrival/departure delays are not profile constraints.

## Implementation and safety

Only notebook 06 changes. Registry and fingerprint are embedded in its source;
there is no runtime dependency on local JSON or an external mutable registry.
The original quality validator and `MAX_INVALID_SERVICE_DATE_RATE=0.001` remain
unchanged. Reviewed rows are excluded from the gate numerator but counted
explicitly in excluded/quarantine diagnostics, never as valid Silver rows.

Conservation requires total = valid-date Silver + reviewed quarantine +
unreviewed invalid rows. Registered quarantine size is at most 32. Registry
corruption, duplicate/foreign quarantine keys, invalid counts, wrong persisted
schema/keys, or ordinary gate violations stop publication.

All existing schema, grain, causal matching (180 seconds), latest-epoch and
output count checks remain in place. Default `allow_silver_overwrite` stays OFF.
With explicit write authorization the order is:

1. Overwrite `rome_transport.silver.trip_updates_quarantine` and verify schema
   and exact key multiset (not merely count).
2. Overwrite/read back `rome_transport.silver.trip_updates`.
3. Overwrite/read back `rome_transport.silver.vehicle_positions`.
4. Overwrite/read back `rome_transport.silver.realtime_trip_stop_observations` LAST.

Separate Delta writes are not atomic across tables. A failure may leave earlier
outputs updated; the observations-last order and existing ALL_SUCCESS chain
protect the dependent job, not independent/manual readers. No new consumer or
concurrent publisher is authorized. Quarantine uses full overwrite, not
append-only audit retention. Bronze remains the unmodified source of truth.

## Local validation and limitations

`python -m unittest discover -s tests -p test_silver06_recovery.py -v` executes
AST-extracted functions and the actual persistence tail from notebook 06 using
small local Spark-expression/transport doubles. It checks real registry keys,
NULL behavior, changed content, original gate, conservation, read-back mismatch,
write failure ordering, write gate OFF and idempotent overwrite.

These are not actual Spark/Delta integration tests. Existing 30 mirror-fixture
test results remain preserved in the external handoff and were not rerun.
No package installation or compute is necessary for local tests.

## Completed Databricks validation

The initial isolated Silver run `1016628906824827` succeeded on 21 November 2026.
The controlled cycle on 22 November also succeeded: Silver
`347496418190254`, enrichment `815655041923187`, scoring input
`1114139366189442`, and operational Gold `46835826666572`.

Silver reported quality PASSED, 32 quarantined rows, two ordinary invalid rows
excluded, zero duplicate observation keys and zero future vehicle positions.
TripUpdates and observations each contained 93,695,962 rows; VehiclePositions
contained 4,864,419 rows. Published Silver versions were 133 / 143 / 194.
Enrichment preserved the observation count. Scoring produced 13,763 rows with
the unchanged 47-feature contract; Gold published manifest version 116 after
its conservation and read-back checks.

These are runtime correctness checks, not evidence of realtime availability:
the source watermark remained 2026-11-22T08:09:39Z and was already stale when
the full cycle completed. They do not validate Neon writes or authorize another
run, deployment or scheduler change.

An independent Silver backup could not be verified within the Free workspace
capacity evidence available. The executed overwrites followed explicit acceptance
that exact rollback to the previous Silver versions was not guaranteed. Do not
assume that time travel or the procedure below guarantees recoverability.

## Deployment and recovery procedure: separate authorization required

The following pre-deployment safeguards remain a procedure for future changes;
their inclusion does not imply that a usable backup or rollback exists today.

1. Review/commit through the canonical Data/ML repository and a reviewed PR;
   no permanent workspace-only change. Preserve source commit and notebook hash.
2. Re-read only relevant job state and active runs immediately before changes.
   Existing downstream job `1084265487272426` was UNPAUSED every 3 hours,
   max_concurrent_runs=1, with ALL_SUCCESS dependencies. The ingestion job
   `195066091322974` was every 20 minutes. These are cached observations, not a
   promise of current state. RMI_ORCHESTRATION_ENABLED=false does not stop them.
3. Obtain explicit authorization for any temporary trigger pause needed to
   prevent automatic execution during workspace update. Do not change it here.
4. Export/preserve the original workspace notebook and record existing Delta
   versions/schemas of all three outputs and quarantine if it already exists.
   Verify retained versions are restorable before allowing overwrites. Capture
   source versions for deterministic replay. Do not VACUUM or change retention.
5. Import only notebook 06, verifying its hash against the reviewed source.
   Changing a workspace notebook used by an active job is an operational change.
6. Obtain explicit authorization for one controlled run, its Free quota use,
   quarantine creation/overwrite and all selected downstream writes. Launching
   the full existing job can execute all four tasks; it is not a 06-only test.

Full rebuild scans historical Bronze and performs matching; 32 quarantined
rows do not imply a cheap 32-row operation. Existing 06 timeout is 2340 seconds;
07/08/13 timeouts were 1680/720/780. This fix adds bounded key collections
(up to 33) and quarantine read-back, but count/validation actions can recompute
large Spark plans. Free usage, execution time and runtime compatibility remain
unmeasured until separately authorized. No paid plan is proposed.

## Controlled sequence: 06 -> 07 -> 08 -> 13

- 06: ordinary gate still passes, expected 32 reviewed rows if the same pinned
  inputs are present; verify conservation, exact quarantine keys, schemas,
  written versions, duplicate count, source max epochs and matching metrics.
- 07: require successful 06; verify it consumes the completed observations,
  enrichment lineage, static coverage and alert/missingness quality.
- 08: require successful 07; verify all 47 unchanged features/contract hash,
  identities, source watermark and preparation timestamp.
- 13: require successful 08; verify Gold lineage and operational denominators.
- Stop on any failed gate/read-back; no manual retry or dependent task bypass.
  Keep GitHub orchestration false. Render validation and persistence require
  later explicit authorization and fresh compatible inputs.

## Rollback: no automatic execution

Before deployment record the original notebook plus exact Delta version for
`trip_updates`, `vehicle_positions`, `realtime_trip_stop_observations` and any
pre-existing quarantine. Do not invent these version numbers from old reports.
If the controlled run fails, stop downstream/concurrent readers under the
approved maintenance procedure and preserve logs/source/written versions.
Restore the original notebook. If partial tables were committed, restore each
affected Delta table to its recorded retained version, observations last,
verify schemas/counts/lineage, then reassess consumers. RESTORE consumes quota
and is itself a write requiring explicit authorization; if versions are not
restorable, stop rather than improvising. A newly created quarantine is retained
for evidence, not automatically dropped. Do not change Bronze, model, threshold,
Neon, Render or GitHub schedules. Reverting code alone does not revert data.

## Cumulative 0.1% gate and quarantine ledger (incremental mode)

The quarantine table keeps its schema but now holds ALL rows with
`service_date IS NULL` (the 32 reviewed rows plus unreviewed invalids). Reviewed
vs unreviewed is derived on read via `reviewed_service_date_exception_expr`; the
32 reviewed keys, hashes and fingerprint are unchanged. The only readers are
notebook 06, `tests/test_silver06_recovery.py` and this document (audited; none
in notebooks 07/08/13, `app/` or SQL).

For an incremental window starting at S (no Bronze scan):
`invalid_hist = ledger(ts<S) - reviewed(ts<S)`, `total_hist = silver(ts<S) + ledger(ts<S)`,
gate = `(invalid_hist + window_invalid) / (total_hist + window_total) <= 0.001`,
identical to full mode. The ledger is rewritten per window with
`replaceWhere feed_timestamp >= S` (no double counting in overlaps); full mode
overwrites it entirely.

Safety (fail closed): the table property `rmi.quarantine_ledger=v1` certifies
that the ledger is coherent with ALL Silver outputs. It is withdrawn before the
first write of ANY run (full or incremental) and set again only after the ledger
and every Silver output (trip_updates, vehicle_positions,
realtime_trip_stop_observations) are written and verified. An absent marker
therefore means "never built" or "interrupted write", and incremental mode
refuses to run (`require_ledger_ready`, reached through
`historical_gate_counts`). Rerunning the same or an earlier window does NOT
reconcile the state and does not unblock it: advancement stays blocked until a
completed full rebuild (default `run_mode=full`) rewrites the ledger and every
Silver table and re-certifies. Cross-table Delta writes are not atomic, so the
marker is the single commit point. Each write carries a unique `userMetadata`
token and the version is found by exactly one match in `DESCRIBE HISTORY` (fail
closed), so a concurrent OPTIMIZE commit cannot be mistaken for the write.

Original checks unchanged: the numerator still excludes the 32 reviewed rows,
`repaired` and the worst-snapshot rate are computed exactly as before (from
`_unreviewed_invalid`), the comparison stays `<= 0.001`, and in full mode the
history terms are 0 so the gate equals the original one. Pinned by
`test_original_gate_semantics_pinned`. The 17 real-Spark tests are skipped
locally (no JVM) and remain UNVERIFIED: no end-to-end equivalence is claimed.

Incremental production writes remain blocked (`INCREMENTAL_WRITE_BLOCKERS`);
`run_mode` defaults to `full`. In the hybrid production Job the preflight task supplies `run_mode` and
`allow_incremental_write=true` (see `DOWNSTREAM_RUNTIME_ROLLOUT.md`).
