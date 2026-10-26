# Rollout downstream controllato

**Stato runtime dopo attivazione del 26 ottobre 2026:** Job downstream `1084265487272426` **ACTIVE**, trigger periodico ogni **2 ore**, 06 → 07 → input scoring 08 → Gold operativo 13. Una run automatica e una run manuale equivalente verificate. Ultimo batch validato: feed **1793015012 / 2026-10-26T12:43:32+01:00**, Silver osservazioni v13, enrichment v13 (57,493,151 righe), scoring v9 (23,039 righe). Gold refresh **2026-10-26T12:43:28.368Z**, qualità PASSED e dashboard_ready YES. La quota Free residua non è verificabile; sostenibilità di lungo periodo e SLA non dimostrati. Ingestion 05 ogni 10 minuti invariata; prediction e metriche ML restano storiche. [Evidenze e limiti](DOWNSTREAM_RUNTIME_ROLLOUT.md).

## Ambito e stato della verifica

Rollout del 25-26 ottobre 2026, a partire da main
`b33bca19efff795555f97c4e9bc7aa72579e6ded`, dopo il merge di PR7.
Ingestion 05 mantenuta indipendente; nessun training, model scoring, aggiornamento delle
etichette, prediction history, FastAPI o n8n. Publish dashboard ed export PDF/screenshot
sono esclusi da questo rollout. Le evidenze distinguono il batch validato dall'ingestion
che continua ad avanzare durante i rebuild.

## Baseline acquisita prima delle scritture

| Livello | Versione Delta | Righe | Ultima epoca feed |
| --- | ---: | ---: | ---: |
| Silver Trip Updates | 10 | 15,402,700 | 1790928849 |
| Silver Vehicle Positions | 5 | 799,479 | 1790928849 |
| Silver osservazioni | 7 | 15,402,700 | 1790928849 |
| Enrichment | 8 | 15,402,700 | 1790928849 |
| Input scoring | 0 | 24,797 | 1790928849 |

Il Job esistente esegue solo 05 ogni 10 minuti, con un solo task, coda e
`max_concurrent_runs=1`. Le ultime cinque run osservate erano SUCCESS. Configurazione
Job, sorgenti runtime, schemi, versioni e metadati Gold sono stati acquisiti prima delle
modifiche. Le sorgenti runtime 06/07/08 coincidevano con il repository; 13 differiva solo
nei commenti, con AST eseguibile identico. I backup operatore restano privati.

Prima del rollout Gold aveva: executive_overview 1 riga, route_reliability 412,
stop_reliability 8,030, temporal_reliability 169, delay_propagation 25,
predictive_operations 17,177, model_performance 14, analytics_refresh_metadata 1.
Il refresh registrato era del 3 ottobre, con evidenze predittive del 2 ottobre.
Il manifest indicava predictive_operations v0, mentre la versione effettiva era v2:
uno stato qualità PASSED preesistente non bastava a certificare la coerenza delle versioni correnti.

## Sicurezza del refresh Silver

06 ricostruisce integralmente tre tabelle: non dispone di watermark incrementale.
Le sorgenti sono ora fissate per versione prima delle azioni. La selezione dell'ultima
ingestion preserva le chiavi logiche e rifiuta payload discordanti a parità dell'ultimo
timestamp; duplicati identici sono intercambiabili. Le posizioni vengono deduplicate
anche sulla chiave `(feed_timestamp, entity_id)`.

Il matching usa posizione e snapshot già disponibili all'epoca della Trip Update, limite
di 180 secondi e ordinamento deterministico dei pareggi. Questo vincolo di pubblicazione
del feed è più conservativo del precedente controllo sul solo timestamp veicolo; una
riduzione di copertura non viene presentata come miglioramento del modello. Le date di
servizio mancanti delle Vehicle Positions restano NULL, come previsto dal parser originale.
Chiavi e date richieste delle Trip Updates, schemi, conteggi, unicità e causalità sono
validati prima degli overwrite. Ogni tabella è poi riletta alla versione appena scritta.

Il primo tentativo si è fermato prima di scrivere perché un controllo era troppo restrittivo
sulle date veicolo opzionali. Dopo la correzione, il primo rebuild completo ha superato
i controlli in 1,113.454 secondi: 56,223,151 osservazioni, zero duplicati, zero posizioni
future, staleness 2-180 secondi e copertura del matching 67.25%.
È stato poi avviato il catch-up alle sorgenti del 26 ottobre, senza confonderlo con questo primo risultato.

## Service Alerts malformati e contesto sconosciuto

Il primo tentativo 07 ha rifiutato prima dell'overwrite intervalli alert con fine precedente
all'inizio. La verifica su Bronze v3543 ha trovato 1,432 righe, due ID alert e 716 snapshot
interessate. Non si trattava di estremi opzionali mancanti: il parser ne conserva già
la presenza tramite `HasField`. Bronze non viene corretto, cancellato o riscritto.

Il nuovo gate `ALLOW_INVALID_ALERT_SNAPSHOT_ISOLATION`, OFF per default, autorizza una
politica conservativa: una snapshot FULL contenente intervalli non validi è contesto
sconosciuto per intero. Il suo timestamp resta nella timeline, così non si recupera una
snapshot precedente. Dopo l'allineamento il timestamp utilizzabile diventa NULL,
`alert_snapshot_available_flag` è false e `alert_snapshot_rejected_flag` rende esplicito
il motivo. Le osservazioni sono tutte preservate; nessuno zero alert viene certificato
su una snapshot rifiutata. Le feature condivise continuano a trattare il contesto non
disponibile come NULL, con contratto e hash delle 47 feature invariati.

Fixture Spark verificano che una snapshot rifiutata diventi sconosciuta e che una valida
non cambi. Conteggi degli intervalli, snapshot isolate e osservazioni interessate sono
registrati nelle evidenze runtime. La politica non rende validi i record Bronze malformati.
Riferimento per gli intervalli: [GTFS-Realtime TimeRange](https://gtfs.org/documentation/realtime/reference/#message-timerange).

## Conflitti con i metadati statici

La verifica della snapshot più recente ha rilevato 37 conflitti di linea e uno di direzione.
Il join storico per corsa/sequenza/fermata poteva associare attributi statici di una linea
diversa quando il provider riusava gli identificativi. Il nuovo controllo preserva gli
identificativi grezzi e quelli statici per audit, aggiunge
`schedule_metadata_conflict_flag`, annulla gli attributi statici discordanti e imposta
`schedule_match_flag=false`. Nessuna osservazione e nessun ritardo grezzo viene eliminato.

Il secondo refresh 07 ha superato i controlli: 56,941,882 righe, zero duplicati, zero
posizioni future e 33,986 righe con metadati statici isolati. I 4,717 conflitti di direzione
sono compresi in tale insieme, non vanno sommati. I controlli pre-scrittura verificano
ogni attributo statico mascherato. La copertura delle posizioni rimane 67.13%; i metadati
statici mancanti salgono a 40,380,323 righe. L'assenza di tali metadati è esplicita e non
viene imputata. La run 07 corretta impiega 833.235 secondi, contro 596.099 della prima run
senza questo isolamento: la proposta di pianificazione considera la misura più recente.

## Gold operativo e riferimenti storici separati

Rieseguire il precedente 13 sul dataset etichettato avrebbe soltanto cambiato il refresh
di aggregati storici. Il percorso `current_operations` usa invece enrichment e staging
validati, fissati per versione. Aggiunge campi `ops_` alle cinque tabelle descrittive,
preservando i valori delle colonne storiche e lasciando invariati predictive_operations
e model_performance. Nessun esito futuro viene creato o riclassificato.

I KPI operativi descrivono il ritardo in arrivo riportato dal feed nella riga corsa-fermata
osservata. Non identificano necessariamente la fermata fisica del veicolo e non sono
arrivi certificati. I tassi usano i ritardi non NULL come denominatore; conteggi totali,
noti e mancanti restano distinti. Le classifiche richiedono almeno 1,000 ritardi noti per
linea e 500 per fermata. I tassi operativi non sono direttamente confrontabili con i
precedenti tassi del proxy futuro sul dataset etichettato.

I valori di ritardo del feed sono preservati, senza clipping o imputazione. Minimo,
massimo e `ops_extreme_delay_count` (>43,200 secondi in valore assoluto) espongono i
valori estremi; la regola di qualità delle etichette future non viene trasferita
implicitamente a un diverso indicatore operativo.

La propagazione operativa descrive coppie di fermate rappresentate nella stessa snapshot.
`ops_transition_share` è la quota di coppie con entrambi i ritardi noti sul totale delle
coppie note; non è la probabilità del futuro ritardo alla fermata successiva. La semantica
ML `next_stop_delay_ge_300s`, la soglia Spark storica p>0.45 e tutte le metriche TEST restano
invariate e separate. La futura soglia sklearn resta da rivalidare.

Gli aggregati compatti hanno un limite di 50,000 righe; le osservazioni sorgente non vengono
raccolte nel processo Python. Anche le snapshot Gold precedenti sono conservate in memoria
entro un limite esplicito di 50,000 righe per tabella, prima delle scritture: il ricaricamento
di una vecchia versione può essere bloccato dalla retention appena non è più corrente.
Non si promette quindi un rollback automatico basato soltanto sui numeri di versione.
Le scritture Gold sono validate mediante rilettura e confronto
completo, e il manifest viene pubblicato per ultimo. `ops_release_id` è comune ai cinque
output e al `refresh_id` finale: le query operative devono rifiutare output non appartenenti
al rilascio validato. Non esiste una transazione atomica tra tutte le tabelle; un errore
parziale richiede l'esclusione del rilascio incompleto, non una dichiarazione di successo.

## Risultati finali del batch validato

Feed downstream **1793001463 / 2026-10-26T07:57:43+00:00**. Sorgenti fissate all’avvio del rebuild: Bronze Trip Updates v3975, Vehicle Positions v3636, alert v3609, schedule v2 e calendar v0. Il feed era di circa due minuti precedente all’avvio di 06. Non è un rilascio live continuo.

| Layer | Versione Delta validata | Righe | Watermark / stato |
| --- | ---: | ---: | --- |
| Bronze Trip Updates, verifica finale | in avanzamento | 57,402,592 | 2026-10-26T09:27:05+00:00 |
| Silver Trip Updates | 12 | 56,941,882 | 2026-10-26T07:57:43+00:00, batch allineato |
| Silver osservazioni | 10 | 56,941,882 | 2026-10-26T07:57:43+00:00, batch allineato |
| Enrichment | 10 | 56,941,882 | 2026-10-26T07:57:43+00:00, batch allineato |
| Input scoring | 5 | 26,327 | 2026-10-26T07:57:43+00:00, batch allineato |

Silver Vehicle Positions v7: 2,962,172 righe. Ultima data di servizio Silver: 2026-10-26; enrichment: 2026-10-26. Gold completato **2026-10-26T09:27:31.249044+00:00**; `quality_status=PASSED`, `dashboard_ready=YES`, `ops_status=PINNED_INPUT_ALIGNED`. Manifest confrontato con le versioni Delta effettive e release ID unico nelle cinque tabelle operative.

| Tabella Gold | Versione | Righe | Ambito |
| --- | ---: | ---: | --- |
| executive_overview | 2 | 1 | Campi ops_ correnti al batch; colonne storiche preservate |
| route_reliability | 2 | 428 | Campi ops_ correnti al batch; colonne storiche preservate |
| stop_reliability | 2 | 8,152 | Campi ops_ correnti al batch; colonne storiche preservate |
| temporal_reliability | 2 | 599 | Campi ops_ correnti al batch; colonne storiche preservate |
| delay_propagation | 2 | 25 | Campi ops_ correnti al batch; colonne storiche preservate |
| predictive_operations | 2 | 17,177 | Evidenza modello storica |
| model_performance | 0 | 14 | Evidenza modello storica |
| analytics_refresh_metadata | 2 | 1 | Manifest operativo con lineage modello storica |

Al controllo **2026-10-26T09:30:38.091Z UTC**, Bronze raggiungeva 1793006825; lag Bronze → Silver **5,362 secondi (89.4 minuti)**. Silver → enrichment **0 secondi**; enrichment → scoring **0 secondi**. Il lag include l’esecuzione sequenziale, la correzione e il secondo refresh 07/08/Gold; non si dichiara un SLA di dieci minuti. Bronze continua ad avanzare. Il manifest riporta il watermark del batch, non lo sostituisce con la data di refresh.

Coverage dello scoring: **305 linee, 1,476 corse, 1,475 veicoli, 6,884 fermate**. Duplicati grain: 0; 47 feature = 42 numeriche + 5 categoriche; hash invariato. Batch `171ad5b0-59a8-4775-9bf7-1fc62d005418`, preparazione `2026-10-26T09:15:04.182075+00:00`. Tutte le feature hanno almeno un valore disponibile; NaN e infiniti: 0.

Aggregati operativi sull’archivio: 28 date di servizio, 428 linee e 8,152 fermate; 55,950,983 ritardi noti e 990,899 mancanti. Ritardo riportato ≥300s: 10,417,852, quota 0.186196 sui ritardi noti. Minimo -86396.0s, massimo 106390.0s, 136,874 valori con modulo >43,200s, preservati e segnalati. Le snapshot alert isolate interessano 12,157,337 osservazioni, che restano nel dataset con contesto alert sconosciuto.

## Health check e readiness delle pagine

Tutte le **10 celle SQL health** e le **27 query dataset dashboard** hanno stato SUCCEEDED dopo l’ultimo refresh Gold. I controlli nativi sui dati completi verificano grain, causalità, conservazione righe/chiavi e rilettura; le query dashboard verificano la compatibilità di lettura con output limitato. Nessuna pubblicazione live eseguita.

| Pagina | Stato dati nel JSON locale |
| --- | --- |
| Overview | CURRENT DATA per KPI operativi; riquadri modello HISTORICAL MODEL EVIDENCE |
| Reliability | CURRENT DATA, fino al watermark del batch |
| Propagation | CURRENT DATA, transizioni nella stessa snapshot |
| Predictive | HISTORICAL MODEL EVIDENCE, 2 ottobre |
| Model Performance | HISTORICAL MODEL EVIDENCE, metriche TEST Spark congelate |

Ingestion continua con run successive riuscite e configurazione invariata. L’audit degli ultimi quattro giorni conserva due errori transitori di decodifica (Trip Updates e Vehicle Positions), successivamente recuperati; non si certifica assenza assoluta di failure. Gli empty feed riusciti restano distinti dai fallimenti. Feature storiche v8, labeled v12 e prediction ML v0 sono invariati; nessun nuovo model scoring o training. Le due tabelle Gold del modello sono state confrontate per valore prima/dopo il refresh.

## Configurazione e benchmark prima dell’attivazione

Job separato **1084265487272426**, `06_13_downstream_operational_refresh`: 06 → 07 → 08 current scoring → 13 current operations. Job ingestion 05 **195066091322974** invariato, ogni 10 minuti. Un solo run downstream concorrente, coda abilitata, dipendenze ALL_SUCCESS e `max_retries=0`; nessun training/model scoring. I gate di scrittura restano OFF nei notebook e sono abilitati esplicitamente nei parametri del Job.

| Stadio | Runtime validazione misurato (s) | Timeout Job (s) |
| --- | ---: | ---: |
| silver_06 | 1143.554 | 2340 |
| enrichment_07 | 833.235 | 1680 |
| scoring_input_08 | 330.756 | 720 |
| operational_gold_13 | 379.032 | 780 |

Somma dei tempi misurati: **44.78 minuti**, senza pause manuali. La durata Gold include il setup; gli altri tempi sono misurati nel notebook. Proposta iniziale **ogni 2 ore, PAUSED**: margine rispetto al benchmark completo, nessuna sovrapposizione ammessa. È una proposta tecnica da confrontare con freshness desiderata e quote Free, non una cadenza già sostenibile/attiva. I limiti residui di quota non sono stati misurati. Dodici rebuild al giorno moltiplicherebbero il costo: la sostenibilità va accettata prima di attivare il trigger.

Il rebuild 06 è completo perché manca un watermark incrementale e il matching deve riconciliare storico e duplicati; 07 e la preparazione delle feature condivise ricostruiscono anch’essi dal contesto completo. Costi e durata possono crescere con lo storico. Una cadenza più stretta richiede un disegno incrementale con parità verificata, in una modifica separata. Persistono copertura statica limitata, alert storici isolati e stime estreme del feed: qualità strutturale PASS non equivale a accuratezza predittiva.

Il JSON dashboard locale è aggiornato e testato; live draft/published non sono stati modificati. PDF e tutti e quattro gli screenshot sono invariati. Il prossimo rollout dashboard deve distribuire e pubblicare il JSON validato, poi esportare il PDF autentico e sostituire overview, propagation, predictive e model-performance con dimensioni e tema previsti. Nessun FastAPI, sklearn, n8n o prediction artificiale è stato creato.

## Verifiche repository

Compilazione di notebooks/contracts e 53 test locali PASS. Eseguiti i blocchi effettivi
del workflow CI: notebook SQL e deliverable richiesti presenti, nessun file vietato
versionato. Verificati diff whitespace, UTF-8, JSON, link/anchor relativi e pattern di
segreti o percorsi privati. `.gitignore` già esclude cache, bytecode, configurazioni e
dati locali; non richiede modifiche. I test sorgente restano versionati. Nessun binario
PDF/PNG e nessun notebook ML 09-12 o helper condiviso delle feature è stato modificato.

## Attivazione e validazione del 26 ottobre

Main HEAD verificato: `a2dcae8f5a858a6ac4e7d08a5be8d67ff51e0a1e`; PR #7-#10 mergiate e working tree pulito. Configurazione e sei sorgenti runtime corrispondevano esattamente alla baseline versionata. Attivazione richiesta **2026-10-26T10:59:16.717093+00:00**, readback **2026-10-26T10:59:19.654941+00:00**: l’unica proprietà runtime modificata è `trigger.pause_status`, da PAUSED a UNPAUSED.

Configurazione finale: trigger periodico **2 HOURS**, max_concurrent_runs=1, coda abilitata, retry Jobs automatici disabilitati. Timeout 06/07/08/13 invariati: **2340 / 1680 / 720 / 780 secondi**. Il benchmark precedente era 44.78 minuti; la somma dei timeout è 92 minuti, inferiore alle due ore, ma queue/setup o crescita dello storico impediscono di trattarla come garanzia assoluta di latenza. Il trigger periodico non espone timezone o un prossimo avvio esatto via jobs/get; non è stato sostituito con cron. Europe/Rome è usato per visualizzare gli istanti, mantenendo gli intervalli periodici della piattaforma.

### Run osservati

| Run | Trigger | Inizio UTC | Fine UTC | Durata (s) |
| --- | --- | --- | --- | ---: |
| 28749734220193 | ONE_TIME | 2026-10-26T11:04:43.835000+00:00 | 2026-10-26T11:55:23.916000+00:00 | 3040.081 |
| 113160267318301 | PERIODIC | 2026-10-26T11:53:35.540000+00:00 | 2026-10-26T12:43:38.752000+00:00 | 3003.212 |

Dopo cinque minuti senza un avvio periodico osservato e senza un orario API disponibile è stata eseguita una sola run manuale equivalente, autorizzata come fallback dopo l’attivazione. Nessun secondo avvio manuale. Una run automatica e una run manuale equivalente verificate.

| Run | Task | Setup (s) | Esecuzione (s) | Retry Jobs | Esito |
| --- | --- | ---: | ---: | ---: | --- |
| 28749734220193 | silver_06 | 4.000 | 1169.000 | 0 | SUCCESS |
| 28749734220193 | enrichment_07 | 2.000 | 825.000 | 0 | SUCCESS |
| 28749734220193 | scoring_input_08 | 2.000 | 342.000 | 0 | SUCCESS |
| 28749734220193 | operational_gold_13 | 2.000 | 690.000 | 0 | SUCCESS |
| 113160267318301 | silver_06 | 119.000 | 1190.000 | 0 | SUCCESS |
| 113160267318301 | enrichment_07 | 2.000 | 845.000 | 0 | SUCCESS |
| 113160267318301 | scoring_input_08 | 2.000 | 330.000 | 0 | SUCCESS |
| 113160267318301 | operational_gold_13 | 2.000 | 509.000 | 0 | SUCCESS |

Nessun overlap di esecuzione tra run downstream e nessun retry Jobs osservato. La run PERIODIC ha queue_duration=115.763s (task 06: 115.473s), con motivo MAX_CONCURRENT_RUNS_REACHED. Il timestamp start_time include la coda: non indica sovrapposizione di compute. La run manuale e gli altri task non espongono queue_duration; valori omessi non sono stati interpretati come zero. Gli stati BLOCKED dei task dipendenti sono attese del DAG, non errori. La coda resta abilitata e max_concurrent_runs=1 resta il vincolo di concorrenza.

### Ultimo batch validato dopo il run

| Layer | Watermark UTC | Righe | Versione Delta |
| --- | --- | ---: | ---: |
| bronze | 2026-10-26T12:56:25+00:00 | 57,904,290 | in avanzamento |
| silver | 2026-10-26T11:43:32+00:00 | 57,493,151 | 13 |
| enrichment | 2026-10-26T11:43:32+00:00 | 57,493,151 | 13 |
| scoring | 2026-10-26T11:43:32+00:00 | 23,039 | 9 |

Gold refresh **2026-10-26T12:43:28.368Z**, quality_status **PASSED**, dashboard_ready **YES**. Manifest confrontato con tutte le versioni Gold effettive; release ID coerente nelle cinque tabelle operative. Schemi invariati. Feature storiche, labeled, prediction ML e le due tabelle Gold del modello non sono state riscritte.

Scoring: 23,039 righe, zero duplicati grain, 47 feature (42 numeriche + 5 categoriche), hash `11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5` invariato, zero NaN/inf e un solo batch/source version/preparazione. Lineage source enrichment v13, scoring v9.

Lag al controllo **2026-10-26T13:00:51.835344+00:00**: Bronze → Silver **4373s**; Silver → enrichment **0s**; enrichment → scoring **0s**. Preparazione scoring → refresh Gold **831.446s**; watermark feed scoring → refresh Gold **3596.368s**. Questi sono due intervalli diversi. Bronze continua ad avanzare durante il full rebuild.

### Quote Free Edition e limite della dimostrazione

La [documentazione ufficiale](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) limita Free Edition a serverless e cinque task Job concorrenti per account; il fair-use può sospendere compute fino al reset giornaliero o, in casi estremi, mensile. Non esiste SLA. Nelle 25 run recenti lette al precheck non comparivano errori quota/resource exhaustion; nessun run era attivo prima della prova.

`system.billing.usage` è leggibile, ma mostra consumi contabilizzati, non l’entitlement Free residuo. Al precheck JOBS riportava 13.734869627 DBU del 26 ottobre fino alle 07:20 UTC: erano dati in ritardo, anteriori a questa attivazione. Non dimostrano headroom sufficiente. La quota residua affidabile resta **NON VERIFICABILE**. Per il nuovo Job le righe di billing non erano ancora disponibili alla verifica post-run; questo non significa costo zero.

Nessun errore quota nei run verificati. Questo prova il funzionamento nell’intervallo osservato, non la sostenibilità di 12 rebuild al giorno o di un mese intero. Occorre continuare a monitorare durata, code, failure e consumo; in caso di quota/sostenibilità il Job va rimesso PAUSED senza retry. Non ridurre l’intervallo sotto due ore. Seconda esecuzione osservata e validata: sì. Batches con nuovi watermark mantengono grain e schemi senza accumulare duplicati; non è una prova di uguaglianza su input identici fissati.

Job 05 invariato. Nessun training, model scoring, sklearn, FastAPI o n8n. Nessuna modifica o nuova pubblicazione della dashboard, né modifica a PDF o screenshot in questa fase: gli export restano snapshot statiche del watermark precedente anche quando il dashboard live legge un nuovo rilascio Gold.

### Confronto baseline e due release

La validazione manuale usa Delta time travel al completamento (`2026-10-26T11:55:23.916Z`); la seconda verifica legge il rilascio PERIODIC. Non sono stati confusi i due snapshot.

| Metrica | Prima | Manuale | PERIODIC |
| --- | --- | --- | --- |
| Feed operativo | 1793001463 | 1793012260 | 1793015012 |
| Righe osservazioni | 56941882 | 57370093 | 57493151 |
| Refresh Gold | 2026-10-26T09:27:31.249Z | 2026-10-26T11:55:12.206Z | 2026-10-26T12:43:28.368Z |
| Delta silver.realtime_trip_stop_observations | 10 | 11 | 13 |
| Delta silver.realtime_enriched_observations | 11 | 12 | 13 |
| Delta features.next_stop_delay_scoring | 5 | 7 | 9 |
| Delta gold.executive_overview | 2 | 3 | 4 |

### Tempi dei task (UTC)

| Run | Task | Start | End | Durata wall (s) | Queue (s) |
| --- | --- | --- | --- | ---: | --- |
| 28749734220193 | silver_06 | 2026-10-26T11:04:43.870000+00:00 | 2026-10-26T11:24:17.317000+00:00 | 1173.447 | non esposta |
| 28749734220193 | enrichment_07 | 2026-10-26T11:24:17.727000+00:00 | 2026-10-26T11:38:05.818000+00:00 | 828.091 | non esposta |
| 28749734220193 | scoring_input_08 | 2026-10-26T11:38:06.269000+00:00 | 2026-10-26T11:43:50.384000+00:00 | 344.115 | non esposta |
| 28749734220193 | operational_gold_13 | 2026-10-26T11:43:50.842000+00:00 | 2026-10-26T11:55:23.420000+00:00 | 692.578 | non esposta |
| 113160267318301 | silver_06 | 2026-10-26T11:53:35.561000+00:00 | 2026-10-26T12:15:25.306000+00:00 | 1309.745 | 115.473 |
| 113160267318301 | enrichment_07 | 2026-10-26T12:15:25.718000+00:00 | 2026-10-26T12:29:33.168000+00:00 | 847.450 | non esposta |
| 113160267318301 | scoring_input_08 | 2026-10-26T12:29:33.561000+00:00 | 2026-10-26T12:35:05.959000+00:00 | 332.398 | non esposta |
| 113160267318301 | operational_gold_13 | 2026-10-26T12:35:06.381000+00:00 | 2026-10-26T12:43:38.295000+00:00 | 511.914 | non esposta |

Lag della release manuale: Bronze snapshot a fine run -> Silver 2752s; Silver -> enrichment 0s; enrichment -> scoring 0s; preparazione scoring -> Gold 1022.068s. Il lag Bronze della verifica PERIODIC usa Bronze corrente al controllo, non il suo timestamp di fine run.
