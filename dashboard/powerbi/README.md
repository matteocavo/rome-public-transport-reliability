# Dashboard Power BI

Versione Power BI destinata alla presentazione pubblica del progetto Rome Public Transport Reliability & Delay Prediction.

**Stato:** pianificata.

Power BI rimane una presentazione separata dall'app React/FastAPI; non sostituisce il
backend operativo. Consumerà Gold curato con date di sorgente e provenienza esplicite.
Metriche e previsioni Spark esistenti sono riferimenti storici, non performance del futuro
modello sklearn. Target >=300 secondi invariato; soglia Spark storica p>0.45, futura soglia
operativa da rivalidare. Nessun report Power BI implementato o rigenerato in questa PR.

Tema condiviso: chiaro istituzionale, bianco/avorio, testo scuro, rosso pompeiano/bordeaux,
oro/ocra e grigi neutri, con contrasto e leggibilità accessibili. Nessun redesign.

Vedi [ADR-001](../../docs/ARCHITECTURE_DECISIONS.md), [roadmap](../../docs/ROADMAP.md)
e [audit dashboard/PDF/screenshot](../databricks/README.md).
