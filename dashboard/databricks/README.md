# Dashboard pubblicata ed export verificati

Il PDF completo e i quattro PNG sono aggiornati all’export autentico fornito il
**26 ottobre 2026**, successivo alla pubblicazione manuale delle **10:38:56.940 UTC**
(11:38:56, Europe/Rome). Sono stati controllati testo e rendering di tutte le cinque
pagine in tema chiaro. La dashboard interattiva richiede autenticazione Databricks.

## Provenienza e distinzione dei dati

La sorgente dashboard è
[`dashboard/Affidabilita_Trasporto_Pubblico_Roma.lvdash.json`](dashboard/Affidabilita_Trasporto_Pubblico_Roma.lvdash.json).
Il percorso operativo proviene da PR #8; la correzione delle tre label asse/display name/
legenda di Propagation è in [PR #9](https://github.com/matteocavo/rome-public-transport-reliability/pull/9),
già deployata e pubblicata manualmente nel workspace. Al momento di questo refresh
PR #9 è aperta: il suo merge deve precedere quello della PR artifact per riallineare
anche il JSON di main. La PR artifact non modifica query o JSON.

| Pagina | Ambito dell’export |
| --- | --- |
| Panoramica Esecutiva | KPI operativi; riquadri predittivi separati come replay Spark storico |
| Affidabilità della Rete | Ritardi riportati alla fermata osservata; tassi e ranking sui ritardi noti |
| Propagazione dei Ritardi | Quote delle coppie precedente/corrente nella stessa snapshot; nessun outcome futuro |
| Monitoraggio Predittivo | Replay Spark storico del 2 ottobre 2026, non inference live |
| Prestazioni del Modello | Metriche TEST storiche Spark/Persistence; FastAPI/sklearn futuro |

Watermark operativo **26 ottobre 2026 08:57:43 Europe/Rome** (epoca 1793001463).
Refresh Gold **26 ottobre 10:27:31 Europe/Rome**. La Panoramica esportata mostra
**160 minuti** di età del feed: è la misura al momento dell’export, non un valore
aggiornato del PDF né uno SLA. Il Job downstream resta in pausa; nessun refresh dati
è stato eseguito per produrre questi artifact. [Evidenze runtime](../../docs/DOWNSTREAM_RUNTIME_ROLLOUT.md).

## Verifica del PDF

[`pdf/Affidabilita_Trasporto_Pubblico_Roma.pdf`](pdf/Affidabilita_Trasporto_Pubblico_Roma.pdf)
conserva byte per byte il nuovo export Databricks: nessuna ricostruzione, overlay o
correzione grafica dei valori. Cinque pagine, nell’ordine della tabella sopra.

- Panoramica: 56,94 milioni di osservazioni, 428 linee, mediana -40s e P95 1.282s;
  watermark e ambito storico dei riquadri predittivi espliciti.
- Propagation: “Quota delle Transizioni” presente su asse e legenda del visual per
  stato attuale, oltre alla heatmap e alla tabella. Vecchia label “Tasso di Ritardi ≥ 5 min”
  assente dalla pagina 3. La descrizione KPI precedentemente tagliata è ora leggibile.
- Predictive: snapshot del 2 ottobre dichiarata, senza nuova inference.
- Modello: F1 TEST 82,83%, ROC-AUC 96,90%, PR-AUC 90,63%; soglia Spark strettamente
  p>0.45, p=0.45 negativo. Soglia sklearn da rivalidare. Persistence TEST: MAE
  136.9818s, RMSE 401.8525s; il vecchio RMSE 489s non compare.
- Stato architetturale: replay storico, qualità del rilascio operativo e target
  FastAPI/sklearn da implementare sono distinti.

I valori estremi del feed nei grafici di recupero/aggravamento sono preservati
dall’export; non sono stati corretti o rimossi dalle immagini. Limiti e semantica
sono descritti nel report runtime.

## Screenshot aggiornati

Tutti i PNG derivano dal nuovo PDF reale; numero di pagina e intestazione esterna
sono esclusi tramite crop. Nessun ritocco ai contenuti. Proporzioni preservate e,
dove necessario, margini bianchi per mantenere le dimensioni originarie.

| Screenshot | Pagina PDF | Dimensioni |
| --- | ---: | --- |
| [overview.png](screenshots/overview.png) | 1 | 2840 × 2340 |
| [propagation.png](screenshots/propagation.png) | 3 | 2840 × 2160 |
| [predictive.png](screenshots/predictive.png) | 4 | 2840 × 2970 |
| [model-performance.png](screenshots/model-performance.png) | 5 | 2840 × 2180 |

Anche propagation è stato sostituito: contenuti, KPI e label sono cambiati rispetto
alla precedente rappresentazione del rischio futuro. Gli artifact sono ora
allineati alla versione pubblicata; non restano export in attesa di rigenerazione.

## Controlli e confini del refresh

Prima del publish manuale: 27 query dataset PASS, readback esatto del draft, layout
e palette invariati; controllo visivo del draft confermato dall’owner. Dopo l’export:
cinque pagine renderizzate e ispezionate, quattro crop verificati, dimensioni e SHA-256
del PDF confrontati con il file fornito. Backup e intermedi restano fuori dal repository.

Questo refresh modifica solo PDF, quattro screenshot e README/PACE/documentazione
dashboard. Nessuna scrittura Gold, pipeline, Job, training, model scoring, FastAPI,
sklearn o n8n. Il publish è stato eseguito manualmente dall’owner; nessun merge
automatico. [ADR-001](../../docs/ARCHITECTURE_DECISIONS.md) resta il riferimento per
il target ML esterno; Power BI è una presentazione separata futura.
