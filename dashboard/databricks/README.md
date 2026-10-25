# Sorgente dashboard ed export storici

Data dell'audit: 25 ottobre 2026. [ADR-001](../../docs/ARCHITECTURE_DECISIONS.md)
sposta il target ML operativo su FastAPI/sklearn, mentre i risultati Gold/ML esistenti
restano evidenze Spark storiche. Questa PR non ha modificato dashboard, query, dati,
draft o snapshot pubblicata nell'ambiente live.

## Risultati dell'audit e modifiche nel repository

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
Lo stato di qualità esistente viene ancora mostrato, riferito al refresh storico; nessun
dato o metrica viene riscritto. I precedenti campi registro/refresh presenti altrove restano
metadati storici. La logica esistente di fasce di rischio/target con 0.45 viene preservata
per il modello storico, senza trasferirla a sklearn.

## Stato di PDF e screenshot

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
**Rigenerazione degli screenshot interessati necessaria: SÌ** (overview, predictive, model-performance).
Dati/design della propagazione sono invariati; conservare lo screenshot a meno che il
controllo qualità dell'export ne richieda la sostituzione. La pagina 3 del vecchio PDF mostra
anche una descrizione KPI tagliata: verificare il difetto esistente durante il rollout
e correggerlo solo con autorizzazione esplicita sul layout.

## Rollout dashboard separato e autorizzato

1. Salvare una copia del draft effettivo e dei metadati/sorgenti pubblicati della dashboard;
   confrontarli con la baseline versionata. Riconciliare le modifiche esterne prima di
   applicare questo JSON. Distribuire solo le modifiche revisionate a testi e query SELECT;
   non modificare Gold, output del modello o layout.
2. Eseguire i dataset dashboard in sola lettura e confrontare valori e date storici.
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
   predictive e model-performance dall'export reale, escludendo come prima numero di
   pagina e interfaccia del browser e conservando le dimensioni: overview 2840x2340,
   predictive 2840x2970, model-performance 2840x2180. Propagation è 2840x2160 se risulta
   necessario sostituirlo.
6. Ispezionare gli output a dimensione piena e confermare la corrispondenza fra PDF,
   sorgente e screenshot. Aggiornare questo stato e le didascalie README/PACE solo dopo
   la sostituzione effettiva, quindi includere gli artifact nel commit della PR di rollout.
   Nessun publish/export dashboard avviene nella PR corrente.

Power BI resta una presentazione separata pianificata; deve adottare la stessa distinzione
storico/corrente e la futura soglia validata. Non esiste un report binario da rigenerare qui.
