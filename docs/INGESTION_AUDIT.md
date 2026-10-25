# Contratto di audit dell'ingestion GTFS-Realtime

## Confine dell'architettura operativa

L'ingestion resta responsabilità di Databricks. L'inventario in sola lettura del 25 ottobre
2026 conferma che il Job periodico esistente esegue solo il notebook 05; non aggiorna
06 Silver, 07 enrichment o 08 di preparazione degli input di scoring. La freschezza dei
dati (freshness) Bronze non implica quindi feature aggiornate. Un DAG downstream e un
contratto di readiness, da autorizzare separatamente, sono specificati nell'[handoff
operativo](OPERATIONAL_ML_HANDOFF.md). n8n coordinerà chiamate FastAPI/retry/alert esterni
dopo la readiness, senza sostituire questa pianificazione ingestion. La PR di handoff
architetturale non modifica impostazioni runtime.

## Ambito e stato del deployment

PR2 è stata integrata come codice del repository senza modifiche runtime. Un rollout controllato autorizzato separatamente il 25 ottobre 2026 ha applicato la migrazione audit additiva e distribuito i notebook 05, 07 e 99 dal merge main `ae7f9ef11724daaf7bb1cd3e9530673d1c7ef9a8`. La run manuale di ingestion in produzione è riuscita; seguono evidenze dettagliate e limiti residui della validazione. Il notebook 05 è il writer di produzione; il notebook 04 resta storico, esplorativo/di bootstrap e non deve essere pianificato insieme a esso. Il notebook 06 è invariato. ML, feature, etichette, Gold e artifact dashboard sono invariati.

Gli schemi delle tre tabelle di entità GTFS e i payload del parser restano invariati. L'estensione audit e l'allineamento degli alert vuoti nel notebook 07 richiedono il rollout esplicito sotto riportato. Non distribuire il notebook 05 prima della migrazione audit: il controllo preliminare verifica nomi/tipi delle colonne e fallisce prima del recupero dalla rete o delle scritture di entità.

## Header, parsing e risposte vuote

L'header protobuf viene letto prima di costruire qualsiasi DataFrame. Una richiesta/decodifica con header inizializzato, versione GTFS non vuota e timestamp esplicito positivo del feed può rappresentare zero entità: SOURCE SUCCESS, entities_received=0, parsed_row_count=0, rows_appended=0, is_empty=true. Non viene inventata alcuna riga di entità. Il requisito del timestamp esplicito è la politica di questa pipeline per la chiave della snapshot; non implica che GTFS richieda ogni campo protobuf opzionale.

L'incrementalità segue il [riferimento ufficiale GTFS-Realtime](https://gtfs.org/documentation/realtime/reference/#enum-incrementality): se omessa, usa il valore predefinito protobuf FULL_DATASET. DIFFERENTIAL esplicito viene rifiutato come INVALID_FEED_METADATA prima di qualsiasi append di entità. Entità eliminate o di tipo inatteso in un feed FULL vengono rifiutate come PARSE_ERROR anziché saltate implicitamente. La modalità full/differential viene validata per ogni payload recuperato; le modalità storiche dei feed romani non sono certificate retroattivamente.

Zero righe analizzate con entità non nulle non viene classificato come feed vuoto. Un'entità TripUpdate può legittimamente non avere aggiornamenti delle fermate. entities_received conta ora le entità protobuf decodificate, non gli ID distinti delle entità analizzate o le righe delle fermate. Il campo separato parsed_row_count preserva questa distinzione.

NETWORK_ERROR, HTTP_ERROR, INCOMPLETE_READ, DECODE_ERROR, PARSE_ERROR, INVALID_FEED_METADATA, WRITE_ERROR e UNKNOWN_ERROR sono categorie stabili. error_message originale viene mantenuto. I metadati noti prima dell'errore restano disponibili; i conteggi sconosciuti di righe analizzate/duplicate sono NULL. Il valore legacy non NULL rows_appended=0 in caso di errore non prova che una scrittura terminata in timeout non possa avere effettuato il commit.

## Migrazione audit additiva - applicata durante il rollout autorizzato

Tabella esistente: rome_transport.bronze.ingestion_runs. Preservare le sette colonne esistenti:
run_timestamp TIMESTAMP, source_name STRING, feed_timestamp BIGINT, entities_received BIGINT, rows_appended BIGINT, status STRING, error_message STRING.

Il rollout autorizzato ha applicato le seguenti nove colonne mancanti dopo aver verificato i tipi di tutte e sette le colonne legacy. Questo SQL resta documentazione; CI e writer non lo eseguono automaticamente:

```sql
ALTER TABLE rome_transport.bronze.ingestion_runs ADD COLUMNS (
  record_type STRING,
  ingestion_batch_id STRING,
  ingestion_attempt_id STRING,
  parsed_row_count BIGINT,
  duplicate_row_count BIGINT,
  is_empty BOOLEAN,
  feed_incrementality STRING,
  decode_status STRING,
  error_type STRING
);
```

Ispezionare prima DESCRIBE e aggiungere solo le colonne mancanti se la migrazione è stata applicata parzialmente. Nessun ALTER, mergeSchema, riscrittura, backfill o creazione di tabella automatici. Le righe legacy conservano NULL nei nuovi campi. I consumer usano COALESCE(record_type, 'SOURCE') per le righe sorgente storiche e non devono inventare incrementalità storica o evidenze di feed vuoto.

I record record_type=SOURCE preservano lo stato sorgente SUCCESS/FAILED. record_type=BATCH usa source_name=__batch__ riservato e gli stati RUNNING/SUCCESS/PARTIAL_SUCCESS/FAILED. I consumer esistenti devono escludere __batch__ nel calcolo dei conteggi per sorgente. Le vecchie query di controllo sono adattate con questa esclusione retrocompatibile; le nuove celle di controllo richiedono le colonne migrate. Non si aggiungono colonne ridondanti entity_count/inserted_count/batch_status.

duplicate_row_count = parsed_row_count - rows_appended include duplicati esatti nel payload e chiavi già presenti in Bronze. I valori si riferiscono al singolo tentativo, non a totali storici ricostruiti. Le righe BATCH non rappresentano una snapshot sorgente: i conteggi sorgente sono segnaposto e non devono mai entrare nelle metriche del feed.

## Esiti del batch e comportamento in caso di errore

Ogni esecuzione aggiunge un record BATCH RUNNING prima dell'acquisizione, poi un esito SOURCE per feed e infine un evento BATCH terminale. I tentativi precedenti non vengono sostituiti.

- SUCCESS: tutte le sorgenti sono riuscite, inclusi feed full vuoti validi.
- PARTIAL_SUCCESS: almeno una sorgente riuscita e una fallita.
- FAILED: tutte le sorgenti fallite, oppure un errore di audit/finalizzazione impedisce un record batch utilizzabile.

Tutte le sorgenti vengono tentate anche dopo un errore sorgente. Dopo la persistenza dell'audit terminale, PARTIAL_SUCCESS e FAILED generano un errore del task, consentendo l'applicazione delle notifiche di errore Job esistenti. Nessuna impostazione Job viene cambiata. Il test runtime autorizzato del percorso di successo è passato. Retry della stessa snapshot e consegna delle notifiche di errore non sono ancora testati in produzione; la propagazione degli errori è coperta da revisione del codice e test offline.

Un errore iniziale di scrittura audit ferma l'esecuzione prima delle scritture sorgente. Un errore successivo di scrittura audit non viene assorbito: le sorgenti restanti vengono tentate e lo stato finale è FAILED. Un errore nella scrittura audit finale propaga. Le scritture tra tabelle non sono atomiche: un arresto del processo o un'indisponibilità dello storage possono ancora lasciare commit di entità senza esito audit; un tentativo con solo RUNNING significa incompleto/sconosciuto. Questa PR non dichiara transazioni audit exactly-once.

## Retry e idempotenza

Le chiavi snapshot esistenti sono preservate: Trip Updates (feed_timestamp, entity_id, stop_sequence, stop_id), posizioni (feed_timestamp, entity_id), alert (feed_timestamp, alert_id). Le righe ripetute esatte nello stesso payload vengono accorpate; payload discordanti su una stessa chiave in ingresso fanno fallire l'esecuzione anziché selezionare una riga arbitraria. L'anti-join è null-safe e impedisce che chiavi fermata NULL aggirino la deduplicazione dei retry. Nessun valore sorgente viene corretto o sovrascritto.

Eseguire un solo writer alla volta. Il Job corrente ha già max_concurrent_runs=1; un writer aggiuntivo, una run del notebook legacy 04 o una run manuale simultanea non sono sicuri senza coordinamento. Delta non impone le chiavi logiche e un anti-join non è un vincolo di unicità tra più writer.

run_realtime_ingestion(ingestion_batch_id=...) accetta un ID logico stabile per retry espliciti. Ogni chiamata crea un nuovo ingestion_attempt_id. Senza ID fornito, ogni invocazione inizia un nuovo batch logico; i retry della piattaforma non sono raggruppati automaticamente nello stesso batch perché il repository non ha un contratto verificato di contesto stabile della run padre. La deduplicazione Bronze resta sicura indipendentemente dal raggruppamento audit. Se serve raggruppamento automatico tra retry, collegare un identificatore stabile della run Job radice in una modifica separatamente approvata a Job/parametri notebook; non assumere che l'ID del tentativo di esecuzione sia stabile. Qui non viene effettuata tale modifica.

## Service Alerts e impatto downstream

Una snapshot FULL_DATASET riuscita con zero alert è evidenza positiva di feed vuoto, diversamente dall'acquisizione mancante. Il notebook 07 aggiunge alla timeline esistente dei timestamp delle righe alert solo i timestamp audit di sorgenti FULL vuote riuscite, così una snapshot vuota più recente sostituisce le vecchie righe alert. Il join di uguaglianza non trova alert a quel timestamp; la copertura resta disponibile con zero alert attivi. DIFFERENTIAL e record audit falliti non sono mai evidenza di uno stato full vuoto. I timestamp audit non vuoti non sono uniti indipendentemente: se più recenti della tabella alert fissata, potrebbero suggerire falsamente zero alert con ingestion concorrente.

In assenza delle colonne audit migrate, il notebook 07 mantiene il comportamento storico basato sulle righe. La presenza parziale dei nuovi campi fa fallire la validazione. Le versioni Delta sorgente, inclusa ingestion_runs quando usata, sono fissate per azioni riproducibili nella run. Le snapshot legacy vuote non si possono ricostruire da zero righe inserite: anche snapshot non vuote ripetute possono inserire zero righe.

PR2 non esegue enrichment. Applicare le nuove evidenze di snapshot vuota richiede un refresh del notebook 07 autorizzato separatamente, che può legittimamente cambiare il contesto degli alert dopo tale snapshot; le feature dipendenti devono poi essere aggiornate separatamente. Output esistenti ed etichette/modelli/Gold storici restano intatti. Si corregge la semantica di disponibilità futura senza cambiare implicitamente i risultati analitici correnti.

## Validazione offline e verifiche runtime successive

I test estraggono le definizioni delle funzioni di produzione tramite AST, evitando l'esecuzione del notebook al livello principale. Oggetti simulati simili a protobuf e sostituti DataFrame in memoria coprono metadati header, zero entità, percorsi di decodifica/errore, stato sorgente/batch, errori audit, ID espliciti di retry, anti-join null-safe e conflitti di duplicati. Non sostituiscono un vero test di integrazione protobuf/Spark/Delta.

La validazione runtime seguente copre compatibilità dello schema, feed FULL non vuoti, persistenza audit e integrità degli append. Verifiche successive residue: retry della stessa snapshot in ambiente Spark/Delta isolato, integrazione reale protobuf per feed vuoti/Differential, esecuzione delle fixture del notebook 07 senza refresh downstream e consegna delle notifiche di errore. Non eseguire le celle esplorative del notebook 04 esistente come procedura di deployment. L'implementazione runtime non richiede nuovi package.


## Evidenze del rollout runtime controllato

Main distribuito: `ae7f9ef11724daaf7bb1cd3e9530673d1c7ef9a8` (merge PR2). Tutte e tre le sorgenti distribuite sono state riesportate e confrontate con main. Gli export runtime 05/07 esistenti coincidevano esattamente con la baseline PR1 prima della sostituzione; ID precedenti degli oggetti e copie delle sorgenti sono stati conservati nel pacchetto di evidenze dell'operatore. Il notebook 99 era assente dalla cartella runtime ed è stato creato da main. Il notebook 04 resta non pianificato/esplorativo; 06 e 08+ non sono cambiati.

Cartella workspace: cartella esistente dei notebook del progetto.

| Notebook | Azione runtime |
| --- | --- |
| `05_gtfs_realtime_pipeline` | Aggiornato ed eseguito dal suo Job esistente |
| `07_gtfs_realtime_enrichment` | Solo aggiornato; nessuna esecuzione dell'enrichment |
| `99_realtime_pipeline_health_check` | Creato; tutte e sette le celle SQL eseguite tramite SQL warehouse |

### Migrazione e baseline di rollback

La versione stabile di `ingestion_runs` prima della migrazione era **10501**, con **10,173 righe** e i sette campi legacy elencati sopra. DESCRIBE TABLE EXTENDED, proprietà della tabella, storico Delta recente e ultimi 20 record audit sono stati acquisiti prima della migrazione. L'ALTER additivo ha creato la versione **10502** senza cambiare il conteggio righe; tutte e nove le nuove colonne erano NULL su tutti i 10,173 record legacy. Non sono stati richiesti backfill, modifiche delle proprietà, OPTIMIZE o VACUUM. L'ottimizzazione automatica Delta esistente compariva nello storico precedente al rollout e non è stata avviata da questo rollout.

Dopo la run manuale l'audit era alla versione **10507**, con **10,178 righe**: esattamente cinque nuovi record. Le sette colonne originali conservano nomi/tipi e le nove aggiunte conservano i tipi documentati.

Non è servito rollback. Il punto di recupero preparato è la versione audit 10501 insieme agli export delle sorgenti notebook precedenti. Se il rollback diventasse necessario, sospendere il trigger e attendere le run attive, preservare le evidenze audit successive, ripristinare il vecchio writer insieme a schema/dati audit compatibili usando un `RESTORE TABLE rome_transport.bronze.ingestion_runs TO VERSION AS OF 10501` autorizzato separatamente, poi reimportare le sorgenti 05/07 salvate. Il notebook 99 era nuovo, quindi non ha revisione precedente. La disponibilità Time Travel va riverificata prima del recupero; il ripristino rimuoverebbe dalla vista corrente i record audit successivi alla baseline, quindi non è un rollback operativo automatico o senza perdita. Le entità Bronze non richiedono rollback per questa run riuscita in solo append. Non riprendere il writer legacy sullo schema esteso senza validare la compatibilità dell'append.

### Run manuale ed esiti audit

La run Job controllata 588842343799032 (evidenze runtime conservate privatamente) è terminata SUCCESS. Run del task: 401724133439920. Batch logico: `b5ea5cf1-e87a-4c3b-bf9b-5b452ea33076`; tentativo: `b72364ae-cfe2-4138-8042-7c443a3d5994`.

Sono stati persistiti esattamente un BATCH RUNNING, tre SOURCE SUCCESS e un BATCH SUCCESS terminale. Ogni sorgente aveva gli stessi ID batch/tentativo non NULL, timestamp feed positivo, `FULL_DATASET`, `DECODED`, `is_empty=false` e campi di errore NULL.

| Sorgente | Timestamp feed | Entità | Righe analizzate | Aggiunte | Duplicati |
| --- | ---: | ---: | ---: | ---: | ---: |
| trip_updates | 1792938901 | 700 | 13,514 | 13,480 | 34 |
| vehicle_positions | 1792939051 | 667 | 667 | 667 | 0 |
| service_alerts | 1792938901 | 128 | 128 | 128 | 0 |

Le 34 righe Trip Update scartate dimostrano l'eliminazione di duplicati in questo input; non costituiscono un test di retry della stessa snapshot.

### Integrità Bronze

La baseline è stata acquisita dopo la conclusione dell'ultima run pianificata del vecchio writer e mentre il trigger era in pausa.

| Tabella | Versione Delta prima / dopo la run manuale | Righe prima / dopo | Gruppi duplicati / righe eccedenti, invariati |
| --- | --- | --- | --- |
| trip_updates | 3874 / 3875 | 55,937,673 / 55,951,153 | 232,698 / 236,180 |
| vehicle_positions | 3533 / 3534 | 2,898,974 / 2,899,641 | 138 / 138 |
| service_alerts | 3501 / 3502 | 517,767 / 517,895 | 0 / 0 |

Tutti e tre gli schemi delle entità coincidevano prima/dopo. `EXCEPT ALL` dalla versione precedente fissata a quella successiva fissata di ogni tabella ha restituito zero righe mancanti o modificate, inclusi payload e campi di ingestion. Gli incrementi dei conteggi coincidevano esattamente con rows_appended delle sorgenti. Il raggruppamento sulle chiavi documentate, incluse le chiavi NULL, non ha mostrato crescita dei duplicati. I duplicati storici sono stati preservati, non riparati o nascosti. I timestamp del feed sono avanzati rispetto alla snapshot precedente.

### Job, controlli di stato e limiti residui

Il Job **195066091322974** ha ancora un task che punta al notebook 05, trigger periodico di 10 minuti, `max_concurrent_runs=1`, coda abilitata e notifica email di errore originale. Il trigger è stato temporaneamente PAUSED, una run del vecchio writer già avviata è stata lasciata terminare e il trigger originale UNPAUSED è stato ripristinato. Il confronto completo delle impostazioni Job prima e dopo il ripristino coincideva esattamente. Nessuna configurazione di compute, task, retry o notifiche è stata modificata permanentemente.

Tutte e sette le celle SQL del notebook 99 sono riuscite. Le query sorgente escludevano `__batch__`; ultimi successi/errori restavano disponibili tra righe legacy e nuove; erano visibili dettagli SOURCE ed entrambi gli eventi BATCH. La query sui feed vuoti riusciti è stata eseguita con successo con zero osservazioni: questo non prova che sia avvenuto un feed vuoto live.

I 20 test offline e la compilazione dei notebook sono passati nuovamente. Sono stati osservati feed FULL non vuoti e scritture Spark/Delta in produzione. FULL vuoti validi, DIFFERENTIAL rifiutati, retry null-safe della stessa snapshot, nuovi ID di tentativo con ID batch stabile fornito e PARTIAL_SUCCESS/errore dopo audit restano supportati dai test offline; non sono stati introdotti intenzionalmente errori o replay in produzione. La consegna delle email di errore non è stata testata. Non è stato creato un notebook Spark isolato separato. Il notebook 07 è stato distribuito ma non eseguito, quindi la timeline degli alert vuoti è verificata tramite codice/test offline e l'output arricchito esistente resta invariato.

Training/scoring/registrazione ML, Gold, dashboard, Power BI e repository dell'app non sono stati toccati. Nessun refresh downstream eseguito.


### Prima osservazione pianificata dopo il ripristino

La run periodica 1030587734568633 (evidenze runtime conservate privatamente) è partita automaticamente dopo il ripristino del trigger ed è terminata SUCCESS. Ha persistito i cinque record attesi con nuovo batch logico `6e821354-466c-4e97-a8f7-f2ed895fe9ef` e tentativo `d0daff62-c731-412f-a022-a72cbe0bb8d9`. Tutte e tre le sorgenti erano SUCCESS, DECODED, FULL_DATASET, non vuote e senza errori; il BATCH terminale era SUCCESS.

| Sorgente | Entità | Analizzate | Aggiunte | Duplicati |
| --- | ---: | ---: | ---: | ---: |
| trip_updates | 684 | 13,627 | 13,579 | 48 |
| vehicle_positions | 684 | 684 | 684 | 0 |
| service_alerts | 128 | 128 | 128 | 0 |

Ogni nuovo timestamp feed era più recente di quello della run manuale. Nelle nuove snapshot non erano presenti chiavi duplicate e gli incrementi totali delle righe coincidevano con i tre conteggi di append. Questo conferma l'ingestion pianificata operativa nella finestra osservata, non garantisce assenza di futuri errori dei feed o dell'infrastruttura. I limiti verificati solo offline sopra indicati restano aperti; non sono stati usati ulteriori replay in produzione o iniezioni di errore.
