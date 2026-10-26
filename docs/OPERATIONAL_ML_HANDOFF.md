# Handoff di dati e accesso per il ML operativo

**Stato runtime al 26 ottobre 2026:** Silver osservazioni v10, enrichment v10 (56,941,882 righe) e input scoring v5 (26,327 righe) allineati al feed **1793001463 / 2026-10-26T08:57:43+01:00**. Gold operativo validato, refresh **2026-10-26T09:27:31.249044+00:00**. Sono dati del batch validato, non una promessa di latenza live. Prediction e metriche Spark restano evidenze storiche. Job downstream separato predisposto, **PAUSED**, con proposta iniziale ogni 2 ore basata sul benchmark di circa 45 minuti; ingestion 05 invariata ogni 10 minuti. [Evidenze, limiti e orchestrazione](DOWNSTREAM_RUNTIME_ROLLOUT.md).


Stato: contratto implementato nel repository; letture dati runtime verificate il 25 ottobre 2026.
Backend, nuovo modello e orchestratori non sono implementati qui. Architettura:
[ADR-001](ARCHITECTURE_DECISIONS.md). Ordine di implementazione: [roadmap](ROADMAP.md).

## Contratti machine-readable

[`contracts/operational_ml.json`](../contracts/operational_ml.json) definisce esplicitamente
l'handoff di training/inference/serving. Il modulo basato sulla libreria standard
[`operational_ml.py`](../contracts/operational_ml.py) genera proiezioni esclusivamente SELECT;
non contiene client di rete, modelli, credenziali, materializzazioni o effetti collaterali di esecuzione.

Entrambi gli export preservano esattamente l'ordine delle **47 feature grezze: 42 numeriche + 5 categoriche**.
Categoriche: `route_id`, `route_type`, `direction_id`, `agency_id`, `current_status`.
I test verificano nomi, ordine e tipi grezzi rispetto a `08_common_feature_preparation`.
Lo SHA-256 mantenuto come provenienza degli input è:

`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`

L'hash include il preprocessing Spark storico. Non obbliga sklearn a riprodurre
imputer/indexer/encoder Spark e non implica un vettore appreso coincidente. I NULL delle
feature opzionali restano NULL; durante l'export non avvengono fit, imputazione o rimozione
di input opzionali. Il futuro preprocessing viene appreso solo sul training, persistito con
l'estimatore e testato su categorie sconosciute/NULL dopo il ricaricamento. Valori infiniti,
colonne mancanti e variazioni di schema/hash fanno fallire la validazione.

### Export di training

Sorgente logica: `rome_transport.features.next_stop_delay_labeled`.
Versione corrente validata: **12**. Riferimento degli esperimenti storici: **8**.
v8 non è più leggibile per i vincoli di retention; il successivo VACUUM è registrato nello storico.
v9-v12 contengono operazioni di manutenzione/metadati e popolazione/etichette di v12 hanno
superato la validazione di compatibilità. È un'evidenza di compatibilità logica, non un
confronto diretto di equivalenza delle righe complete v8/v12. Non è consentito alcun
fallback automatico a una versione Delta più recente.

La proiezione esplicita include identità dell'osservazione, data/ora di servizio, identità
della fermata successiva target, tutte le feature grezze, ritardo realizzato/etichetta di
classe e audit di timestamp/orizzonte dell'etichetta. Identità/etichette/provenienza non
sono mai input dell'estimatore. I metadati registrano tabella/versione sorgente, hash,
confini delle partizioni, cutoff di disponibilità delle etichette, SHA del codice e
manifest di export/campionamento.

Evento di business: `next_stop_delay_ge_300s`, display **Ritardo ≥ 5 min**.
Etichetta positiva: `target_realized_next_stop_delay_seconds >= 300`.
Mantenere `ABS(delay) <= 43200`, chiavi di osservazione valide, orizzonte 1-1800 secondi,
`label_source_feed_timestamp > feed_timestamp` e coerenza esatta dell'orizzonte.
La classe memorizzata deve uguagliare quella derivata; contare le righe rifiutate e
bloccare il rilascio per esclusioni inattese, senza ridefinire implicitamente la popolazione validata.

`training_select(version=12, start_epoch=..., end_epoch=..., label_available_before=...)`
richiede limiti interi non negativi espliciti, intervallo del feed semiaperto e cutoff
stretto di disponibilità delle etichette. Riconciliare conteggi di input e righe
accettate/escluse di ogni partizione. Per train, il cutoff delle etichette è l'inizio di
validation; per validation, l'inizio di test; per test, il cutoff di valutazione entro
cui le osservazioni sono complete. Chiavi mancanti o duplicate bloccano il rilascio dell'export.

### Export di inference

Sorgente: `rome_transport.features.next_stop_delay_scoring`; granularità:
`(feed_timestamp, entity_id, trip_id, stop_sequence, stop_id)`.
Usare `inference_select(version=..., feed_timestamp=...)`, fissando entrambi i valori da
un'unica preparazione validata. Non usare mai `SELECT *`, la tabella etichettata o un
fallback a un'epoca precedente.

L'allowlist coincide esattamente con quella PR3: identità dell'osservazione, della fermata
successiva target e di servizio/veicolo, 47 feature e quattro campi di lineage:
`source_silver_version`, `feature_prepared_at`, `feature_preparation_batch_id`,
`feature_contract_hash`. L'identità `target_*` nella stessa snapshot è un metadato,
non un esito futuro. L'export aggiunge inoltre `export_source_version` fissata,
`exported_at` e `feed_age_seconds`; confrontare le epoche in UTC e visualizzare Europe/Rome.
Sono **assenti etichette future, esiti realizzati, previsioni storiche e output predittivi**.

Snapshot storica PR3: **v0, 24,797 righe, epoca 1790928849**, 2 ottobre 2026 10:14:09 Europe/Rome. La validazione corrente usa **v5, 26,327 righe, epoca 1793001463**, 2026-10-26T08:57:43+01:00. Sono input osservabili aggiornati al batch, non prediction né inference live. Versioni e lineage sono nel [report runtime](DOWNSTREAM_RUNTIME_ROLLOUT.md).
L'incidenza dei valori mancanti per alcuni input/categorie opzionali è circa 75-79%; uno
schema valido non dimostra una qualità del modello accettabile su questa popolazione.

Controlli di rilascio del backend: schema/ordine/hash esatti; una sola lineage/epoca di
preparazione non NULL; conteggio atteso, chiavi di osservazione non NULL/uniche, sequenza
successiva > sequenza corrente; nessuna etichetta; nessun valore numerico infinito;
sorgente/preparazione non future; anzianità del feed e della preparazione entro uno SLA
approvato esplicitamente. Questa PR non inventa uno SLA. Dati non aggiornati/vuoti/incompleti
restituiscono uno stato esplicito di indisponibilità o mancato aggiornamento e non vengono
mai presentati implicitamente come correnti. Il solo orario di preparazione non prova che
la sorgente sia aggiornata.

## Accesso consigliato: SQL Statement Execution API

Usare il SQL warehouse serverless esistente e HTTPS autenticato
`POST /api/2.0/sql/statements` con sole SELECT/DESCRIBE/SHOW. Non sono necessari nuovi
connector/SDK, cluster o export di file. Le risposte di metadati piccole usano INLINE
JSON_ARRAY. Gli export di feature grandi usano **EXTERNAL_LINKS + ARROW_STREAM**, con
un budget in byte. [Contratto API](https://docs.databricks.com/api/statement-execution/v1/statement-execution)
e [guida al download dei chunk](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial) ufficiali.

Test osservati in sola lettura: aggregati del training v12 fissato riusciti; lettura dello
scoring v0 fissato riuscita; lettura Gold della panoramica esecutiva riuscita. Una query
di scoring su due righe/cinque chiavi ha restituito un chunk Arrow (1,840 byte), senza
troncamento; l'URL firmato è stato scaricato con HTTP 200 senza header Authorization.
Questo prova che il canale funziona ora, non il throughput di un export completo,
l'autenticazione del futuro backend o il rinnovo senza intervento umano. È stato usato
solo il profilo locale autenticato esistente; nessun token o URL firmato è versionato
o registrato pubblicamente. Anche entrambe le SELECT generate dai contratti sono riuscite
sulle versioni fissate (test su due righe): 67 colonne di training e 70 di inference,
inclusi i metadati di export/freschezza dei dati (freshness).

Requisiti del futuro backend:

1. Fornire host/warehouse/autenticazione tramite configurazione privata di deployment.
   Usare un metodo di autenticazione del workspace supportato ed effettivamente disponibile
   nell'account; verificarne il rinnovo senza intervento umano prima di abilitare la
   pianificazione. La sola autenticazione interattiva dello sviluppatore non prova la
   disponibilità del servizio. Non esporre credenziali ai log React/n8n.
2. Inviare proiezioni fisse e parametri associati/versioni intere validate. Fissare ogni
   sorgente una volta. Eseguire polling di PENDING/RUNNING fino a uno stato terminale con
   timeout e annullamento; ritentare errori transitori di trasporto/limiti di frequenza/quote
   con backoff limitato, non gli errori di schema.
3. Recuperare tutti i chunk del manifest e verificare schema, righe/byte totali, offset
   contigui, unicità e `truncated == false`. INLINE ha un limite di 25 MiB; i risultati
   esterni suddivisi in chunk supportano fino a 100 GiB. Un limite esplicito di righe/byte
   può troncare i risultati: rifiutare export parziali. Polling o recupero non devono
   applicare implicitamente il limite di 1,000 righe dell'helper di audit.
4. Scaricare i link esterni firmati a breve durata senza inoltrare Authorization di Databricks.
   I link sono segreti e scadono entro 15 minuti; rinnovarli tramite API quando necessario.
   Decodificare Arrow in batch limitati, preservando categorie intere nullable, timestamp
   UTC e date. Validare ogni chunk e il manifest finale prima di promuovere un dataset in cache.
5. Memorizzare in cache un export immutabile riuscito una sola volta per manifest di
   tabella/versione/proiezione/partizioni/campionamento. Salvare Arrow/Parquet interoperabile
   fuori da Git nello storage del futuro backend. I training successivi riusano l'export
   validato; non scaricare dieci milioni di righe per ogni fit.

L'avvio automatico del warehouse provocato da una SELECT è normale esecuzione in lettura;
questo audit non ne ha modificato le impostazioni. Free Edition dispone di un warehouse
2X-Small, quote di utilizzo equo e nessuno SLA. Non dichiarare disponibilità commerciale
di produzione. Hosting, credenziali e accettazione della capacità del backend sono attività
di deployment separate, con test espliciti anziché un servizio presunto disponibile.

## Dimensioni misurate del training e strategia di selezione

Misure su v12 fissata (aggregati lato server sull'intera tabella; nessun download locale massivo):

| Misura | Risultato |
| --- | ---: |
| Righe ammissibili | 9,975,703 |
| Positive / negative | 1,878,569 / 8,097,134 |
| Quota positiva | 18.8314% |
| Date di servizio / linee / corse / veicoli | 8 / 412 / 82,936 / 2,157 |
| Epoche del feed | 964 |
| File Delta attivi / byte compressi dell'intera tabella | 23 / 1,207,872,358 |
| Dimensione JSON delle 47 feature come array di stringhe, byte medi / massimi per riga | 337.134 / 405 |
| Strati non vuoti data × linea × feed_hour × classe | 60,856 |
| Righe per strato: minimo / p10 / mediana / p90 / massimo | 1 / 9 / 97 / 403 / 1,816 |

La stima JSON converte i valori delle feature in stringhe e include i NULL; è un proxy,
non una misura dei byte Arrow, della RAM pandas o dell'artifact. Implica circa **3.36 GB**
di trasferimento delle sole feature prima dei metadati. Una matrice densa float64 delle
sole 42 numeriche occupa **3.35 GB**; il vettore denso storico a 475 dimensioni occuperebbe
**37.91 GB**, prima di copie o overhead dell'estimatore. Nessuna delle due misure è una
raccomandazione a costruire tale matrice.

| Data di servizio | Negative | Positive | Linee negative / positive | Ore negative / positive |
| --- | ---: | ---: | --- | --- |
| 2026-09-25 | 756,150 | 200,639 | 370 / 308 | 16 / 16 |
| 2026-09-26 | 1,040,654 | 245,536 | 371 / 313 | 24 / 24 |
| 2026-09-27 | 716,529 | 184,724 | 334 / 274 | 23 / 23 |
| 2026-09-28 | 1,389,286 | 237,113 | 377 / 330 | 23 / 23 |
| 2026-09-29 | 1,283,348 | 320,915 | 378 / 331 | 24 / 24 |
| 2026-09-30 | 1,105,064 | 307,933 | 368 / 319 | 24 / 24 |
| 2026-10-01 | 1,384,897 | 332,251 | 373 / 328 | 24 / 24 |
| 2026-10-02 | 421,206 | 49,458 | 335 / 247 | 7 / 7 |

Raccomandazione: creare un rilascio curato di training Delta autorizzato separatamente,
con versione/manifest immutabili, quindi memorizzare in cache un export limitato.
Non materializzare nulla in questa PR. Non sono stati selezionati percentuale di
campionamento, modello o budget finale di righe.

La prossima modifica al disegno del training deve:

1. Assegnare **tutte le epoche distinte del feed in ordine cronologico** a train/validation/test
   prima del campionamento, mantenendo unite le epoche coincidenti. Congelare i confini
   senza osservare le prestazioni TEST; escludere con purging le etichette non ancora
   disponibili al confine con la partizione successiva. Preservare la preparazione causale
   delle feature sull'intero storico prima di qualsiasi campionamento; non ricalcolare
   mai lo storico sul sottoinsieme campionato o soltanto sulla snapshot corrente.
2. Misurare byte/riga di un test pilota Arrow limitato, espansione del preprocessing
   nullable/sparso, picco di memoria residente e memoria del fit delle implementazioni
   candidate effettive sull'host backend scelto. Derivare la capacità in righe dalla memoria
   utilizzabile dell'host meno overhead misurato e riserva esplicita, oltre ai budget di
   trasferimento/tempo. Registrare la capacità misurata; nessun `toPandas()` ingenuo
   sull'intera tabella e nessuna percentuale di campionamento ipotizzata senza evidenze.
3. Solo all'interno di TRAIN, allocare quote proporzionali deterministiche sugli strati
   osservati data/linea/ora/classe, preservando ogni strato non vuoto quando la capacità
   lo consente. Ordinare tramite seed registrato e hash stabile della chiave di osservazione,
   usando la chiave completa per risolvere i pareggi. Registrare SQL/algoritmo hash esatti,
   quote, probabilità di inclusione, versione sorgente, righe rifiutate e checksum delle
   chiavi selezionate. Se la quota minima di una riga supera la capacità, rifiutare il budget
   proposto e rivedere esplicitamente il disegno; non perdere implicitamente linee, date,
   positivi rari od ore. Le quote minime possono alterare le proporzioni di classe:
   documentare i pesi di campionamento e valutare le scelte di pesatura su validation.
4. Preservare la prevalenza naturale delle classi nella validation/TEST temporale completa
   quando la capacità lo consente, oppure usare un campione di valutazione riproducibile
   specificato indipendentemente, con pesi di inclusione e incertezza riportata.
   La stratificazione non deve mai spostare osservazioni tra partizioni temporali.
   Riportare copertura ed entrambe le classi per ogni partizione.
5. Selezionare modello/preprocessing/calibrazione e soglia usando solo TRAIN/validation;
   usare date successive non ancora utilizzate per il nuovo TEST finale. Otto date storiche
   e dati TEST già valutati non dimostrano nuova generalizzazione indipendente. Persistire
   l'intera pipeline sklearn con la propria provenienza e provare ricaricamento/parità prima di inference.

## Semantica ed evidenze storiche

`historical_model_threshold = 0.45`, confronto **strettamente >** (uguaglianza negativa).
`future_operational_model_threshold = null`: **da validare nella valutazione del nuovo modello**.
I 300 secondi del target non cambiano. Fasce di rischio e logica del flag storiche restano
regole di visualizzazione di riferimento, non garanzie calibrate o soglie automatiche per sklearn.

**Metriche TEST storiche di riferimento**, solo Spark Random Forest:
Accuracy 0.9401; precision 0.8450; recall 0.8123; F1 0.8283; ROC-AUC 0.9690; PR-AUC 0.9063.
La regressione Persistence resta un riferimento storico (MAE 136.9818s, RMSE 401.8525s,
R² 0.6940, WAPE 0.3293). Nessuna metrica è attribuita a un modello esterno non addestrato.

## Divario di freshness e disegno dell'orchestrazione interna

L'inventario Job di baseline del 25 ottobre conteneva un solo Job di ingestion pianificato, con
**un solo task che punta a 05**, periodico ogni 10 minuti e non in pausa. Non risultano
dipendenze dei task 06/07/08 o altri Job downstream nell'elenco. Il notebook 05 esegue
solo append Bronze/audit e non invoca notebook downstream. Il notebook 06 sovrascrive
gli output Silver quando viene eseguito esplicitamente; 07 fissa gli input e sovrascrive
l'arricchimento; `08_current_scoring_dataset` richiede un gate esplicito di scrittura
staging, disabilitato per default. Nessun commit Delta esegue automaticamente questi notebook.

Nella baseline del 25 ottobre, ultima epoca Bronze Trip Update osservata: **1792960055** (25 ottobre); osservazioni Silver,
arricchimento e staging restano tutti a **1790928849** (2 ottobre). L'arricchimento è v8
e lo staging v0. Questo dimostra l'assenza di orchestrazione downstream, non un'interruzione
dell'ingestion. Non prova che ogni sorgente sia continuamente sana. I risultati Gold/modello
esistenti sono storici.

Disegno logico di baseline. Il rollout del 26 ottobre ha validato 06/07/08 e Gold operativo
in un Job separato da 05: non è stata implementata una dipendenza dal successo dell'ultima
run ingestion. Le letture fissano le versioni disponibili; watermark e anomalie delle sorgenti
restano nei controlli health. La politica di freshness per l'attivazione va ancora accettata.

```text
05 ingestion + audit terminale
  -> verifica successo sorgenti richieste / politica esplicita per sorgenti parziali
  -> 06 Silver
  -> 07 enrichment (posizioni causali <=180s; evidenze di feed alert vuoto)
  -> 08_current_scoring_dataset (helper 08 condiviso; gate di scrittura autorizzato esplicitamente)
  -> validazione e pubblicazione del manifest di readiness
```

Non eseguire il vecchio adapter 08 di training, 09 di etichettatura o 10-12 ML alla cadenza
dell'ingestion. Rilasci di training/maturazione delle etichette usano un workflow offline
separato. Refresh Gold descrittivo e futuro refresh del serving predittivo sono rilasci
separati e versionati; il nuovo 13 distingue current_operations dal riferimento storico. Il percorso operativo aggiorna cinque aggregati e il manifest, senza scrivere le due tabelle del modello. Non usa output di modelli esterni.

Prima di abilitare questo DAG: salvare copie di sorgenti/Job, misurare il costo di
06/07/feature sull'intero storico rispetto all'intervallo di 10 minuti e alle quote Free;
risolvere le letture multisorgente non fissate di 06, il fuso orario di sessione e la
gestione dei duplicati storici noti; riconciliare le versioni e rifiutare chiavi ambigue.
Implementare versioni fissate di batch/sorgenti e un solo writer, coordinamento con
l'ingestion attiva, retry limitati dei task, gestione idempotente dello stesso batch,
assenza di sovrapposizioni e controlli delle notifiche. Non promettere uno SLA downstream
di dieci minuti prima di misurarlo. Se il costo della ricostruzione completa è eccessivo,
implementare e testare la parità di storico/stato incrementale in una modifica separata
prima di abilitare la pianificazione. Gli overwrite Silver completi non sono incrementali.

La readiness deve includere batch/hash di preparazione, versioni Bronze/Silver/staging,
watermark di sorgenti/feed, conteggi, stato di qualità e istante di completamento.
Pubblicare solo dopo tutti i controlli; tabelle parzialmente aggiornate non costituiscono
un rilascio riuscito. Conservare l'ultimo manifest valido per diagnosi, ma marcarlo come
non aggiornato quando lo SLA non è rispettato. Qui non viene creata una nuova tabella di readiness.

n8n verifica readiness e politica di freshness approvata, deduplica
`(staging_version, preparation_batch_id, model_version)`, chiama FastAPI, ritenta errori
transitori con la stessa chiave di idempotenza e genera alert per run non aggiornate/fallite.
Non duplica la pianificazione ingestion Databricks né esegue trasformazioni Spark.
FastAPI ricontrolla il manifest prima di inference; i controlli n8n non sono un confine
di integrità. Storico delle prediction e commit del serving downstream richiedono un
proprio contratto di rilascio idempotente.

## Mappatura serving e confine di rilascio

Questi sono **nomi delle future risorse FastAPI**, non endpoint esistenti. La stessa
mappatura è machine-readable nel JSON. Gold ora separa i campi operativi `ops_` dalle colonne di riferimento storico; le risorse predittive e di valutazione restano storiche.

| Tabella Databricks | Risorsa futura | Interpretazione |
| --- | --- | --- |
| `gold.executive_overview` | `/network/overview` | Snapshot KPI di rete; date storiche e delle previsioni separate |
| `gold.route_reliability` | `/reliability/routes` | Campi ops_: tassi sui ritardi noti, minimo 1,000 ritardi noti per i ranking |
| `gold.stop_reliability` | `/reliability/stops` | Campi ops_ della fermata osservata, minimo 500 ritardi noti per i ranking |
| `gold.temporal_reliability` | `/reliability/temporal` | Data di servizio/giorno/ora del feed |
| `features.next_stop_delay_scoring` | `/scoring/inputs` | Input osservabile del batch validato, con versione fissata; nessuna prediction |
| `gold.predictive_operations` | `/predictions/reference` | Replay Spark storico, mai previsioni correnti esterne |
| `gold.model_performance` | `/models/reference/evaluation` | Metriche storiche congelate |
| `ml.model_registry_metadata` | `/models/reference/metadata` | Metadati storici di modello/run/tentativo |
| `gold.analytics_refresh_metadata` | `/monitoring/analytics` | ID del refresh, versioni sorgente, manifest Gold |
| `bronze.ingestion_runs` | `/monitoring/ingestion` | Eventi di audit SOURCE/BATCH in solo append |

Tutti i nomi sopra usano il catalogo `rome_transport`. Verificare `gold_versions_json`
rispetto alle versioni Delta Gold effettive prima di esporre un rilascio coerente;
fissare ogni tabella della mappatura al manifest. Un flag di qualità positivo non prova
la freshness corrente. Paginare/filtrare le risorse grandi con chiavi stabili, query in
allowlist e parametri associati; non esporre SQL arbitrario. I soli tentativo/stato non
identificano univocamente gli eventi di audit: retry, ID legacy NULL e più eventi
terminali/di errore richiedono aggregazioni consapevoli degli eventi.

Le future previsioni operative e gli endpoint modello/valutazione devono derivare dal
nuovo artifact/valutazione e dallo storico delle prediction progettato separatamente.
Non si dichiara qui una nuova tabella serving o un endpoint. Le 17,177 previsioni storiche
esistenti restano intatte e non sono sostituite dalle 26,327 righe di input osservabile. Schema,
versione del modello, soglia validata, freshness, identità, lineage della run e ID di
rilascio devono accompagnare il futuro output predittivo.

## Accettazione runtime separata

La precedente PR di handoff non modificava il runtime. Il rollout successivo del 26 ottobre ha ora aggiornato Silver, enrichment, input scoring e Gold operativo, predisponendo un Job downstream in pausa. Modelli, dashboard live e publish restano invariati. Attività successive necessarie: verificare quote e attivazione del Job downstream in pausa; approvare hosting/autenticazione/capacità del backend; rilasciare un
export curato; implementare/valutare/persistire e ricaricare il modello esterno; inference
indipendente; poi storico delle prediction, adattamento serving e n8n. Correzioni alla
sorgente dashboard e sostituzione necessaria di PDF/screenshot sono tracciate
nell'[audit dashboard](../dashboard/databricks/README.md).

## Stato del rollout dati del 26 ottobre

**Stato runtime al 26 ottobre 2026:** Silver osservazioni v10, enrichment v10 (56,941,882 righe) e input scoring v5 (26,327 righe) allineati al feed **1793001463 / 2026-10-26T08:57:43+01:00**. Gold operativo validato, refresh **2026-10-26T09:27:31.249044+00:00**. Sono dati del batch validato, non una promessa di latenza live. Prediction e metriche Spark restano evidenze storiche. Job downstream separato predisposto, **PAUSED**, con proposta iniziale ogni 2 ore basata sul benchmark di circa 45 minuti; ingestion 05 invariata ogni 10 minuti. [Evidenze, limiti e orchestrazione](DOWNSTREAM_RUNTIME_ROLLOUT.md).

Il nuovo percorso Gold preserva i campi storici, aggiungendo `ops_` e il controllo del rilascio validato. I KPI operativi sono stime di ritardo correnti del feed; non ridefiniscono l'evento ML futuro. I backend futuri devono leggere esplicitamente i campi e il manifest del corretto ambito, senza assumere che una data di refresh renda attuali le metriche del modello. Il training etichettato resta v12, con la copertura storica di otto date; 09-12 non sono stati rieseguiti.
