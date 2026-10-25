# Operational roadmap

Decision: [ADR-001](ARCHITECTURE_DECISIONS.md). Contracts and acceptance criteria:
[operational handoff](OPERATIONAL_ML_HANDOFF.md). Each runtime rollout needs separate
authorization; this PR performs repository changes and read-only data verification only.

| Order | Deliverable | Acceptance / ownership |
| --- | --- | --- |
| A | Operational ML handoff (this PR) | Frozen raw input contracts, measured export strategy, historical/operational separation, dashboard audit; no backend |
| B | Databricks downstream freshness/orchestration | Measured, version-pinned 05 -> 06 -> 07 -> current-scoring 08; quality/readiness manifest, retries/no-overlap and approved freshness SLA; separate runtime rollout |
| C | Create `rome-mobility-intelligence` | Future separate repository; FastAPI/sklearn, n8n definitions, React/UI; not created by this PR |
| D | Python training/evaluation/persistence | Capacity-based curated dataset, temporal purging, new selection/calibration/threshold validation, immutable complete artifact and native reload/parity |
| E | FastAPI inference | Independent input-only path, validated model/contract, freshness checks, unknown/NULL tests and no fit at inference |
| F | Prediction history | Idempotent append/release, model/threshold/source lineage, reconciliation, evaluation maturity; never overwrite reference replay silently |
| G | n8n orchestration | Readiness polling, idempotent backend calls, retry/backoff/alerts; no duplicate ingestion schedule or Spark transformations |
| H | Serving/API completion | Version-coherent Gold/readiness/model/prediction resources, explicit stale/unavailable states, pagination/auth |
| I | React + Google AI Studio | Accessible light institutional frontend consuming APIs; Google AI Studio UX/UI generation/iteration, no fabricated live data |
| J | AI insight layer | Evidence-linked summaries with source dates and freshness, after API/data quality acceptance |
| K | Power BI | Separate portfolio presentation over curated Gold; not the operational app |

Dashboard text/config alignment is repository-prepared in A. Live draft deployment,
publish, PDF export and affected screenshot replacement are a separate authorized rollout
described [here](../dashboard/databricks/README.md). Until then the binaries are historical
evidence with disclosed known issues, not current architecture documentation.

No new numbered Spark production PR5 is implied: prediction persistence belongs to F
after the external model is evaluated. Existing PR1-PR4 history, tests and notebooks remain
research/reference evidence. No component in the target requires paid classic ML compute.
