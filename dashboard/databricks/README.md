# Dashboard source and historical exports

Audit date: 25 October 2026. [ADR-001](../../docs/ARCHITECTURE_DECISIONS.md)
changes the operational ML target to FastAPI/sklearn, while existing Gold/ML results
remain historical Spark evidence. No live dashboard, query, data, draft or published
snapshot was edited by this PR.

## Audit results and repository changes

The five-page JSON, complete PDF and all four screenshots were inspected, including
PDF text and rendered page/screenshot overviews. They use the intended light institutional
theme: white/light ivory, dark text, bordeaux/Pompeian red, gold/ochre, neutral greys.
No palette, layout, dimensions, dataset names, chart encodings or visual identity changed.

| Location | Existing claim / problem | Local JSON correction |
| --- | --- | --- |
| Overview, page 1 | Model risk for latest available update can imply current service | Explicit historical Spark replay, 2 October 2026, not live inference |
| Predictive, page 4 | Most recent disruption risk can imply current predictions | Explicit historical Spark replay, 2 October 2026 |
| Model, page 5 | Final model/deployment without historical scope | Historical Spark/Persistence; future external FastAPI/sklearn target |
| Page 5 validation vs TEST | 83.74%/83.89% incorrectly described as validation vs TEST | Remove interpretation; values describe validation thresholds, historical TEST F1 remains 82.83% |
| Page 5 threshold note | 0.45 could imply future backend default | Mark historical Spark threshold; sklearn threshold must be revalidated |
| Page 5 Persistence note | RMSE 489s inconsistent with final historical TEST 401.8525s | Correct historical TEST MAE/RMSE; current-delay rule and -71s fallback |
| Page 5 deployment dataset | Hardcoded configured scoring, ready dashboard and deferred Spark registration | Historical replay/refresh quality; external target, no live/deployed claim |

The `model_deployment` query in the **repository JSON only** now describes the existing
historical release and future architecture. It remains SELECT-only over Gold. Existing
quality status is still shown, scoped to the historical refresh; no data/metric is rewritten.
The old registry/refresh fields elsewhere remain historical metadata. Existing 0.45
risk-band/target logic is preserved for the historical model, not transferred to sklearn.

## PDF and screenshot status

**Historical artifacts, pending replacement.** The committed PDF and screenshots predate
these corrections. They are retained unchanged as historical portfolio evidence and are
explicitly labeled as such in README/PACE. They are not the corrected JSON's rendered
output and must not be presented as current architecture or live service. In particular,
`model-performance.png` still contains the old deployment, F1 interpretation and RMSE notes;
`predictive.png` and `overview.png` retain the ambiguous freshness wording.

This PR cannot truthfully produce a new Databricks export without the separately authorized
dashboard rollout. No PDF is synthesized to impersonate a published dashboard, and no
text is painted onto the old screenshots. **Full PDF regeneration required: YES.**
**Affected screenshot regeneration required: YES** (overview, predictive, model-performance).
Propagation data/design is unchanged; preserve its screenshot unless export QA finds it
needs replacement. The old PDF page 3 also shows a clipped KPI description: verify that
existing export defect during rollout and fix only with explicit layout authorization.

## Separate authorized dashboard rollout

1. Back up the actual dashboard draft and published metadata/source; compare with the
   committed baseline. Reconcile external edits before applying this JSON. Deploy only
   reviewed text and SELECT-query changes; do not change Gold, model outputs or layout.
2. Run the read-only dashboard datasets, compare historical values and dates. Check all
   changed text at normal browser size and PDF export scale for clipping/readability.
   If text needs more room, stop and obtain layout scope rather than silently redesign.
3. Explicitly publish the corrected **current draft**. Re-read published metadata/source
   and verify the modified notes/query are present in the published snapshot, not draft only.
4. Export the complete five-page PDF from that published version in the light theme.
   Verify all pages, target >=300s, historical p>0.45 scope, new threshold undecided,
   historical dates, TEST values and external architecture notes.
5. Replace `pdf/Affidabilita_Trasporto_Pubblico_Roma.pdf`. Regenerate overview, predictive
   and model-performance PNGs from the real export, excluding page-number/browser chrome
   as before and preserving dimensions: overview 2840x2340, predictive 2840x2970,
   model-performance 2840x2180. Propagation is 2840x2160 if replacement proves necessary.
6. Inspect the full-size outputs and confirm PDF/source/screenshot agreement. Update this
   status and README/PACE captions only after actual replacement, then commit the artifact
   changes in that rollout PR. No dashboard publish/exports occur in the current PR.

Power BI remains a separate planned presentation; it must use the same historical/current
distinction and validated future threshold. It has no binary report to regenerate here.
