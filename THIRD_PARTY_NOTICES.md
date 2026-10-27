# Third-party notices and licensing scope

## Original code

Original Databricks SQL/PySpark notebooks, operational ML contracts, tests,
dashboard specifications and documentation:

Copyright 2026 Matteo Cavo

Licensed under Apache License 2.0 unless otherwise noted. See [LICENSE](LICENSE).
This grant covers original project work, not third-party components or data.

## Public transport data

Fonte dati: Roma Capitale / Roma Servizi per la Mobilità S.r.l. — dati GTFS e GTFS-Realtime del trasporto pubblico locale (ATAC S.p.A. / Roma TPL).

Source: https://romamobilita.it/it/tecnologie/open-data

Public transport data, values and metrics are not relicensed under Apache-2.0.
They remain subject to the applicable upstream open-data licenses, including
CC BY 4.0 / IODL 2.0 where applicable. Consult the source dataset's terms for
its applicable license, attribution requirements and updates. This includes raw
GTFS, stop names, coordinates, route identifiers, timetable values and metrics
derived from public data; Apache-2.0 applies to the original implementation,
not a replacement license for these underlying data.

## Operational ML contract

The original `contracts/operational_ml.json` contract in the core repository and
its synchronized application copy `app/contracts/operational_ml.json` are
Apache-2.0 original project work. Underlying transit data semantics and values
retain their upstream data licensing; synchronization does not change that policy.

## Dashboard screenshots and PDF

For `dashboard/databricks/screenshots/` and `dashboard/databricks/pdf/`, original
layout, queries, dashboard specification and explanatory text are project work
covered by Apache-2.0. Displayed data and metrics retain the applicable upstream
open-data license and attribution requirements. Third-party interface elements,
logos, trademarks and other embedded assets remain subject to their respective
owners' rights and are not relicensed by this project.

This project is an independent analytical and engineering project and is not affiliated with or endorsed by Roma Capitale, Roma Servizi per la Mobilità S.r.l., or ATAC S.p.A.

## External dependencies and services

Databricks, Spark/PySpark, MLflow, Python libraries and other third-party
components retain their upstream licenses and service terms. Consult the exact
version's bundled license and notices before redistribution. The root license
does not relicense external services, dependencies or data assets.
