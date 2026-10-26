# Sorgente dashboard: dati operativi ed evidenze storiche del modello

Data dell'audit: 25 ottobre 2026. [ADR-001](../../docs/ARCHITECTURE_DECISIONS.md)
sposta il target ML operativo su FastAPI/sklearn. Il successivo rollout dati del 26 ottobre aggiorna Gold operativo, mentre prediction e metriche ML restano evidenze Spark storiche. Il precedente audit non modificava il runtime. Il rollout dati successivo aggiorna Gold, ma non il draft o la snapshot dashboard pubblicata.

## Risultati dell'audit e modifiche al repository

Sono stati ispezionati il JSON delle cinque pagine, il PDF completo e tutti e quattro gli
screenshot, inclusi il testo PDF e le viste d'insieme delle pagine renderizzate e degli
screenshot. Usano il tema istituzionale chiaro previsto: bianco/avorio chiaro, testo scuro,
bordeaux/rosso pompeiano, oro/ocra, grigi neutri. Nessuna modifica a palette, layout,
dimensioni, nomi dei dataset, codifiche grafiche o identità visiva.

| Posizione | Affermazione esistente / problema | Correzione nel JSON locale |
| --- | --- | --- |
| Panoramica, pagina 1 | Il rischio del modello per l'ultimo aggiornamento disponibile può suggerire un servizio corrente | Replay Spark esplicitamente storico, 2 ottobre 2026, non inference live |
| Monitoraggio predittivo, pagina 4 | Il rischio di disservizio più recente può suggerire previsioni correnti | Replay Spark esplicitamente storico, 2 ottobre 2026 |
| Modello, pagina 5 | Modello finale/deployment senza contesto storico | Spark/Persistence storici; futuro target esterno FastAPI/sklearn |
| Pagina 5, validation e TEST | 83.74%/83.89% descritti erroneamente come validation e TEST | Rimossa l'interpretazione; i valori descrivono le soglie su validation, l'F1 TEST storico resta 82.83% |
| Pagina 5, nota sulla soglia | 0.45 può suggerire un valore predefinito del futuro backend | Soglia Spark indicata come storica; soglia sklearn da rivalidare |
| Pagina 5, nota Persistence | RMSE 489s incoerente con il TEST storico finale 401.8525s | MAE/RMSE TEST storici corretti; regola del ritardo corrente e fallback -71s |
| Pagina 5, dataset deployment | Scoring configurato, dashboard pronta e registrazione Spark rinviata impostati nel testo | Replay/qualità del refresh storici; target esterno, nessuna affermazione di servizio live o modello operativo distribuito |

La query `model_deployment`, **solo nel JSON del repository**, descrive ora il rilascio
storico esistente e l'architettura futura. Resta una query esclusivamente SELECT su Gold.
La query distingue ora lo stato qualità del rilascio operativo dai metadati del modello storico. Nessuna metrica del modello viene riscritta; il manifest conserva separatamente il timestamp del refresh storico. La logica esistente di fasce di rischio/target con 0.45 viene preservata
per il modello storico, senza trasferirla a sklearn.

## Stato del PDF e degli screenshot

**Artifact storici, in attesa di sostituzione.** Il PDF e gli screenshot versionati precedono
queste correzioni. Restano invariati come evidenze storiche del portfolio e sono indicati
esplicitamente come tali in README/PACE. Non sono il risultato renderizzato del JSON
corretto e non vanno presentati come architettura corrente o servizio live. In particolare,
`model-performance.png` contiene ancora le vecchie note su deployment, interpretazione F1
e RMSE; `predictive.png` e `overview.png` mantengono la formulazione ambigua sulla freshness.

Questa PR non può produrre un nuovo export Databricks autentico senza il rollout dashboard
autorizzato separatamente. Nessun PDF viene sintetizzato per simulare una dashboard
pubblicata e nessun testo viene sovrapposto ai vecchi screenshot.
**Rigenerazione del PDF completo necessaria: SÌ.**
**Rigenerazione degli screenshot interessati necessaria: SÌ** (overview, predictive, model-performance e propagation).
Nel rollout dati successivo è cambiata anche la propagazione: ora espone quote delle coppie osservate, non il rischio futuro storico. Anche propagation deve quindi essere rigenerato dopo il publish. La pagina 3 del vecchio PDF mostra
anche una descrizione KPI tagliata: verificare il difetto esistente durante il rollout
e correggerlo solo con autorizzazione esplicita sul layout.

## Rollout separato e autorizzato della dashboard

1. Salvare una copia del draft effettivo e dei metadati/sorgenti pubblicati della dashboard;
   confrontarli con la baseline versionata. Riconciliare le modifiche esterne prima di
   applicare questo JSON. Distribuire solo le modifiche revisionate a testi e query SELECT;
   non modificare Gold, output del modello o layout.
2. Eseguire i dataset dashboard in sola lettura e confrontare watermark e valori operativi, oltre a date e metriche storiche del modello.
   Verificare tutti i testi modificati alla dimensione normale del browser e alla scala
   dell'export PDF, controllando tagli e leggibilità. Se serve più spazio, fermarsi e
   ottenere l'autorizzazione sul layout, senza ridisegnarlo implicitamente.
3. Pubblicare esplicitamente il **draft corrente corretto**. Rileggere metadati/sorgenti
   pubblicati e verificare che note/query modificate siano nella snapshot pubblicata,
   non soltanto nel draft.
4. Esportare il PDF completo di cinque pagine dalla versione pubblicata, in tema chiaro.
   Verificare tutte le pagine, target >=300s, ambito storico di p>0.45, nuova soglia ancora
   da decidere, date storiche, valori TEST e note sull'architettura esterna.
5. Sostituire `pdf/Affidabilita_Trasporto_Pubblico_Roma.pdf`. Rigenerare i PNG overview,
   predictive, model-performance e propagation dall'export reale, escludendo come prima numero di
   pagina e interfaccia del browser e conservando le dimensioni: overview 2840x2340,
   predictive 2840x2970, model-performance 2840x2180. Propagation va ora sostituito, mantenendo 2840x2160.
6. Ispezionare gli output a dimensione piena e confermare la corrispondenza fra PDF,
   sorgente e screenshot. Aggiornare questo stato e le didascalie README/PACE solo dopo
   la sostituzione effettiva, quindi includere gli artifact nel commit della PR di rollout.
   Nessun publish/export dashboard avviene nella PR corrente.

Power BI resta una presentazione separata pianificata; deve adottare la stessa distinzione
storico/corrente e la futura soglia validata. Non esiste un report binario da rigenerare qui.

## Riallineamento dati del 26 ottobre

**Stato runtime al 26 ottobre 2026:** Silver osservazioni v10, enrichment v10 (56,941,882 righe) e input scoring v5 (26,327 righe) allineati al feed **1793001463 / 2026-10-26T08:57:43+01:00**. Gold operativo validato, refresh **2026-10-26T09:27:31.249044+00:00**. Sono dati del batch validato, non una promessa di latenza live. Prediction e metriche Spark restano evidenze storiche. Job downstream separato predisposto, **PAUSED**, con proposta iniziale ogni 2 ore basata sul benchmark di circa 45 minuti; ingestion 05 invariata ogni 10 minuti. [Evidenze, limiti e orchestrazione](../../docs/DOWNSTREAM_RUNTIME_ROLLOUT.md).

Le 27 query del JSON locale sono state eseguite in sola lettura sul nuovo Gold. Panoramica, Affidabilità e Propagazione leggono i campi `ops_`, con controllo del rilascio validato; il riquadro predittivo della Panoramica e le pagine Predittivo/Modello restano evidenze storiche. Le query pubblicate live non sono state sostituite: finché non si distribuisce e pubblica il JSON corretto, la dashboard esistente continua a usare il vecchio contratto. Nessun PDF/screenshot è stato modificato. Il prossimo export deve sostituire il PDF completo e tutti e quattro gli screenshot, mantenendo tema, dimensioni e controlli sul clipping di pagina 3.
