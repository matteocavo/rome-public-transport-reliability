# Architecture decisions

## ADR-001: operational ML outside Databricks Free Edition

**Accepted: 25 October 2026.** Operational ML target: **EXTERNAL PYTHON BACKEND TARGET**.
Spark ML status: **HISTORICAL / EXPERIMENTAL ML - RESEARCH / REFERENCE IMPLEMENTATION**.
This decision replaces the proposed PR4 production deployment path; it does not invalidate
the historical experiments, TEST metrics or replay predictions.

### Evidence and decision

The workspace is Free Edition, serverless-only. Its observed Spark Connect session
could not construct `VectorAssembler`: the minimal serialization smoke stopped with
`Py4JSecurityException` (constructor whitelist), before model fit or serialization.
Setting explicit MLflow tracking URI `databricks` and registry URI `databricks-uc`
allowed experiment selection, run creation and parameter logging. Tracking success
did not establish model persistence. An isolated Volume write probe also passed.

The single authorized 16.4 LTS ML CPU Dedicated compute creation was rejected:
`Current organization ... does not have any associated worker environments`.
No classic cluster was created, no production fit ran and no production PR4 artifact
was persisted or reloaded. No compute remains running from that attempt. Historical
Spark/MLflow experiments and failed smoke evidence are preserved outside public debug dumps.

We choose a runtime boundary that works without paid classic compute: Databricks
prepares and serves versioned data contracts; a future FastAPI/Python/scikit-learn
backend owns operational training, evaluation, persistence, loading and inference.
No automatic conversion of the historical Spark Random Forest is intended. Algorithms,
preprocessing, calibration, hyperparameters and probability thresholds require new
temporal evaluation. This PR supplies contracts and SQL generators, not a running backend.

### Responsibilities

| Component | Ownership |
| --- | --- |
| This Databricks repository | GTFS ingestion, Delta Bronze/Silver, causal features, labels, analytics, Gold, data contracts and freshness metadata |
| Future `rome-mobility-intelligence` repository | FastAPI, sklearn training/evaluation, complete fitted artifacts/config, immutable model versions, loading, inference and APIs |
| n8n, in the future repository | Cross-system schedule, readiness checks, backend invocation, bounded retry, alerts and workflow definitions/exports |
| React + Google AI Studio | App frontend; Google AI Studio generates/iterates UX/UI, React consumes APIs and displays provenance/freshness |
| Databricks AI/BI / Power BI | Lakehouse analytics; Power BI remains a separate planned presentation, not the app |

The future app repository is not created or initialized by this PR. Public documentation
uses the repository name rather than a developer's local filesystem location.

### Consequences and acceptance

Historical notebooks 09-12 and PR4 helpers/callers remain available with their original
logic and disabled PR4 gates. Their Spark-specific contracts are reference evidence,
not future operational deployment instructions. Shared observable preparation remains
the source of the 47 raw features. The existing hash includes historical preprocessing
settings: retain it for input provenance, and give the future sklearn artifact its own
preprocessing/artifact version. Do not claim Spark/sklearn model equivalence.

No core target component requires Spark ML persistence or UC model registration.
Implementation is still future work, with explicit acceptance gates rather than a claim
of present end-to-end operation. Free Edition quotas and the external backend's hosting
remain availability constraints; there is no commercial SLA or promise of continuous live service.
Fresh downstream data, a newly evaluated model, persisted artifact reload/parity and
independent inference must pass before any live operational claim.

See [handoff contracts, access, measurements and rollout](OPERATIONAL_ML_HANDOFF.md),
[roadmap](ROADMAP.md) and [dashboard audit](../dashboard/databricks/README.md).

### Sources

- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations): serverless-only compute, one small SQL warehouse, fair-use quotas and no SLA.
- [Statement Execution API](https://docs.databricks.com/api/statement-execution/v1/statement-execution): supported SQL access and chunked result formats.
- The controlled runtime observations above were recorded on 25 October 2026; they
  describe this workspace, not a universal limitation of all Spark ML deployments.
