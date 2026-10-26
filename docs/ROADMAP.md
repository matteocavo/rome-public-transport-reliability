# Roadmap operativa

Decisione: [ADR-001](ARCHITECTURE_DECISIONS.md). Contratti e criteri di accettazione:
[handoff operativo](OPERATIONAL_ML_HANDOFF.md). Ogni rollout runtime richiede
un'autorizzazione separata; il rollout dati del 26 ottobre è stato autorizzato e validato, con il nuovo trigger downstream ancora in pausa.

| Ordine | Risultato previsto | Criteri di accettazione / responsabilità |
| --- | --- | --- |
| A | Handoff ML operativo (questa PR) | Contratti degli input grezzi congelati, strategia di export basata su misure, separazione storico/operativo, audit dashboard; nessun backend |
| B | Freshness e orchestrazione downstream Databricks, validazione manuale completata | Ingestion 05 indipendente; 06 -> 07 -> input scoring 08 -> Gold operativo validati il 26 ottobre. Job separato PAUSED, proposta 2 ore da benchmark; nessuno SLA ancora approvato e quote Free da verificare prima dell'attivazione |
| C | Creazione di `rome-mobility-intelligence` | Futuro repository separato; FastAPI/sklearn, definizioni n8n, React/UI; non creato da questa PR |
| D | Training/valutazione/persistenza Python | Dataset curato dimensionato sulla capacità, purging temporale, nuova selezione/calibrazione/validazione della soglia, artifact completo immutabile e ricaricamento nativo/parità |
| E | Inference FastAPI | Percorso indipendente basato solo sugli input, modello/contratto validati, controlli di freshness, test su categorie sconosciute/NULL e nessun fit durante inference |
| F | Storico delle prediction | Append/rilascio idempotenti, lineage di modello/soglia/sorgente, riconciliazione, maturazione della valutazione; nessuna sovrascrittura silenziosa del replay di riferimento |
| G | Orchestrazione n8n | Polling della readiness, chiamate backend idempotenti, retry/backoff/alert; nessuna duplicazione della pianificazione ingestion o delle trasformazioni Spark |
| H | Completamento serving/API | Risorse Gold/readiness/modello/prediction coerenti per versione, stati espliciti di dati non aggiornati/non disponibili, paginazione/autenticazione |
| I | React + Google AI Studio | Frontend istituzionale chiaro e accessibile che consuma API; generazione/iterazione UX/UI con Google AI Studio, nessun dato live inventato |
| J | Livello di insight AI | Sintesi collegate alle evidenze, con date delle sorgenti e freshness, dopo l'accettazione delle API e della qualità dei dati |
| K | Power BI | Presentazione portfolio separata su Gold curato; non l'app operativa |

L'allineamento di testi/configurazione dashboard è preparato nel repository in A.
Deployment del draft live, publish, export PDF e sostituzione degli screenshot interessati
richiedono un rollout separato autorizzato, descritto [qui](../dashboard/databricks/README.md).
Fino ad allora i file binari sono evidenze storiche con problemi noti dichiarati,
non documentazione dell'architettura corrente.

Non si introduce una nuova PR5 numerata per il runtime operativo Spark: la persistenza
delle prediction appartiene a F, dopo la valutazione del modello esterno. Storia PR1-PR4,
test e notebook esistenti restano evidenze di ricerca/riferimento. Nessun componente
del target richiede compute ML classic a pagamento.

**Stato runtime al 26 ottobre 2026:** Silver osservazioni v10, enrichment v10 (56,941,882 righe) e input scoring v5 (26,327 righe) allineati al feed **1793001463 / 2026-10-26T08:57:43+01:00**. Gold operativo validato, refresh **2026-10-26T09:27:31.249044+00:00**. Sono dati del batch validato, non una promessa di latenza live. Prediction e metriche Spark restano evidenze storiche. Job downstream separato predisposto, **PAUSED**, con proposta iniziale ogni 2 ore basata sul benchmark di circa 45 minuti; ingestion 05 invariata ogni 10 minuti. [Evidenze, limiti e orchestrazione](DOWNSTREAM_RUNTIME_ROLLOUT.md).
