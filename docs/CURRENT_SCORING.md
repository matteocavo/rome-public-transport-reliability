# Feature condivise e input dell'export di inference (PR3)

**Architettura corrente:** Databricks gestisce la preparazione degli input osservabili; il futuro backend FastAPI/sklearn gestisce l'inference del modello operativo. [ADR-001](ARCHITECTURE_DECISIONS.md) sostituisce il percorso operativo Spark proposto. I [contratti di export training/inference](OPERATIONAL_ML_HANDOFF.md) preservano ordine delle 47 feature e identità senza etichette future. La soglia storica di probabilità Spark 0.45 non diventa automaticamente la soglia del futuro backend.

PR3 è stata distribuita e validata in un rollout controllato autorizzato separatamente il 25 ottobre 2026 da main `c343f1e8420a801bab98d22f298c939f4b8c0ae0`. La preparazione condivisa ha superato la parità Spark vecchio/nuovo; lo staging degli input è stato creato una sola volta e riletto con corrispondenza esatta. Run MLflow, modelli, prediction, Gold e output dashboard esistenti sono invariati. Il notebook 12 resta refit/replay storico; l'implementazione PR4 di persistenza Spark è preservata come riferimento, non come target operativo. **L'ultimo input Silver disponibile è del 2 ottobre, non un feed live aggiornato.**

## Architettura e selezione

`silver.realtime_enriched_observations` a una versione Delta fissata alimenta il notebook di definizioni condivise `08_common_feature_preparation` tramite una cella `%run` separata. L'helper da solo non esegue letture, scritture, fit o etichettatura.

- Training: il notebook 08 chiama il costruttore condiviso, aggiunge i campi audit provvisori originali della stessa snapshot e preserva proiezione originale della tabella delle feature e filtro di ammissibilità. Solo il notebook 09 associa osservazioni future e crea etichette proxy realizzate. I notebook 09–12 sono invariati.
- Input corrente: il notebook `08_current_scoring_dataset` legge direttamente Silver, limita lo storico all'epoca del feed selezionata, calcola le feature condivise su tutte le osservazioni passate/correnti valide e seleziona poi una snapshot del feed. Non legge mai le tabelle delle feature/etichette né richiede un valore di ritardo alla fermata successiva.

L'epoca predefinita è il timestamp massimo del feed Silver non successivo all'istante di preparazione. È possibile richiedere un'epoca esplicita per la validazione controllata. Non avviene fallback a una popolazione di scoring non vuota precedente. Una snapshot senza righe ammissibili blocca l'overwrite. L'ultima Silver disponibile non è necessariamente aggiornata: l'anzianità del feed viene stampata e questa PR non inventa uno SLA di freschezza dei dati (freshness) né aggiorna Silver.

## Contratto dell'osservazione e della fermata successiva

Tabella staging: `rome_transport.features.next_stop_delay_scoring`. La granularità resta `(feed_timestamp, entity_id, trip_id, stop_sequence, stop_id)`.

Ogni riga rappresenta una stima riportata per corsa-fermata, osservata all'epoca del feed. `stop_id/stop_sequence` identificano quell'osservazione, non necessariamente la posizione fisica dell'autobus. Gli ID target identificano la fermata successiva rappresentata nella stessa snapshot di corsa/entità/data di servizio/orario di partenza/veicolo. Feed sparsi possono saltare fermate programmate. Una riga richiede identificatori della fermata successiva con sequenza crescente, ma mai un ritardo successivo, una snapshot futura o un'etichetta. Le righe terminali sono escluse; una fermata successiva rappresentata con ritardo NULL resta ammissibile.

I campi originali del veicolo `current_stop_sequence/current_status` e quello opzionale `vehicle_current_stop_id` restano distinti dalla fermata dell'osservazione. Non viene introdotta una selezione arbitraria di una sola fermata per veicolo. ID fermata ripetuti su una corsa circolare sono ammessi quando le sequenze differiscono.

Lo schema staging contiene identificatori di osservazione/veicolo/servizio, i cinque campi di identità target della stessa snapshot, le 47 feature grezze ordinate del modello e quattro campi di lineage. I campi di identità target sono metadati audit, non feature assemblate del modello; non viene persistito ritardo target, etichetta di classificazione, esito provvisorio o provenienza dell'etichetta. `SCORING_COLUMNS` è l'allowlist esplicita dell'output. Il controllo delle colonne future del notebook legacy 12 non viene riusato per lo staging; l'inference esterna deve preservare questa separazione esplicita fra metadati di identità e input grezzi del modello.

## Contratto delle feature congelato

I metadati runtime del registro sono stati letti senza modifiche. Il loro JSON esatto e l'ordine delle feature sono incorporati nell'helper. Run MLflow sorgente: `a9b568f478344d9dba90436358f14092`.

SHA-256 prima e dopo:
`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`

42 feature numeriche precedono 5 categoriche, nell'ordine esistente. L'hashing preserva elenco originale delle colonne vietate e impostazioni di preprocessing; non ridefinisce il modello. I tipi grezzi attesi sono stati confrontati con lo schema runtime esistente delle feature. Sono ammessi tipi numerici Spark compatibili con la proiezione DOUBLE congelata; route_type/direction_id restano categorie intere e route_id/agency_id/current_status restano stringhe. Ogni colonna del modello deve esistere. I NULL sono ammessi, inclusi input opzionali interamente NULL; imputer/indexer già appresi li gestiscono successivamente. Nessuna nuova imputazione, fit o codifica. Tipi incompatibili, colonne del modello mancanti, hash discordante, chiavi duplicate/incoerenti e provenienza non causale bloccano la persistenza. I valori infiniti restano invalidi; i valori numerici NaN/NULL sono lasciati all'imputer mediano congelato.

`trip_progress_ratio` resta NULL nella preparazione condivisa perché non è disponibile l'estensione statica completa della corsa. È mantenuto come colonna tecnica nell'output di training legacy, ma è assente sia dall'elenco congelato delle feature del modello sia dalla proiezione corrente di scoring.

## Matrice di audit feature/sorgente/leakage

Di seguito tutti gli input grezzi del modello nell'ordine del contratto. Tutti sono usati dal modello. La base temporale è la disponibilità all'istante del feed, non una prova della disponibilità storica all'istante di ingestion. I valori delle fermate precedenti nella stessa snapshot sono stime contemporanee del feed, non esiti certificati di fermate completate.

| Feature | Tipo grezzo | Sorgente / base temporale | Futura o derivata dal target? |
| --- | --- | --- | --- |
| `feed_hour` | int | feed_datetime corrente, Europe/Rome | No |
| `feed_minute` | int | feed_datetime corrente, Europe/Rome | No |
| `day_of_week` | int | service_date corrente | No |
| `is_weekend` | int | service_date corrente | No |
| `minutes_since_midnight` | int | feed_datetime corrente, Europe/Rome | No |
| `current_arrival_delay_seconds` | int | Stima feed di arrivo/partenza della fermata corrente a t | No |
| `current_departure_delay_seconds` | int | Stima feed di arrivo/partenza della fermata corrente a t | No |
| `abs_current_delay_seconds` | int | Stima feed di arrivo/partenza della fermata corrente a t | No |
| `is_currently_delayed_flag` | int | Stima feed di arrivo/partenza della fermata corrente a t | No |
| `is_currently_early_flag` | int | Stima feed di arrivo/partenza della fermata corrente a t | No |
| `lag_1_arrival_delay_seconds` | int | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `lag_2_arrival_delay_seconds` | int | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `lag_3_arrival_delay_seconds` | int | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `rolling_mean_delay_last_3_stops` | double | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `rolling_max_delay_last_3_stops` | int | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `rolling_std_delay_last_3_stops` | double | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `delay_change_from_previous_stop` | int | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `delay_acceleration` | int | Fermate corrente/precedenti rappresentate nella stessa snapshot; nessun ritardo successivo | No |
| `scheduled_seconds_to_next_stop` | int | Orari/coordinate statici della fermata corrente e successiva rappresentata a t | No |
| `distance_to_next_stop_meters` | double | Orari/coordinate statici della fermata corrente e successiva rappresentata a t | No |
| `vehicle_speed` | double | Associazione causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |
| `vehicle_bearing` | double | Associazione causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |
| `vehicle_odometer` | double | Associazione causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |
| `current_stop_sequence` | int | Associazione causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |
| `vehicle_position_staleness_seconds` | bigint | Associazione causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |
| `vehicle_position_available_flag` | int | Associazione causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |
| `active_alert_flag` | int | Ultima snapshot alert prima o all'istante del feed; disponibilità sconosciuta resta NULL | No |
| `active_alert_count` | int | Ultima snapshot alert prima o all'istante del feed; disponibilità sconosciuta resta NULL | No |
| `active_detour_flag` | int | Ultima snapshot alert prima o all'istante del feed; disponibilità sconosciuta resta NULL | No |
| `active_construction_flag` | int | Ultima snapshot alert prima o all'istante del feed; disponibilità sconosciuta resta NULL | No |
| `historical_route_mean_delay` | double | Stime di arrivo corrente da epoche feed strettamente precedenti a t; epoche coincidenti escluse | No |
| `historical_stop_mean_delay` | double | Stime di arrivo corrente da epoche feed strettamente precedenti a t; epoche coincidenti escluse | No |
| `historical_route_stop_mean_delay` | double | Stime di arrivo corrente da epoche feed strettamente precedenti a t; epoche coincidenti escluse | No |
| `historical_route_hour_mean_delay` | double | Stime di arrivo corrente da epoche feed strettamente precedenti a t; epoche coincidenti escluse | No |
| `historical_route_observation_count` | bigint | Stime di arrivo corrente da epoche feed strettamente precedenti a t; epoche coincidenti escluse | No |
| `historical_stop_observation_count` | bigint | Stime di arrivo corrente da epoche feed strettamente precedenti a t; epoche coincidenti escluse | No |
| `missing_vehicle_position` | int | Disponibilità del corrispondente input/storico osservabile | No |
| `missing_current_arrival_delay` | int | Disponibilità del corrispondente input/storico osservabile | No |
| `missing_current_departure_delay` | int | Disponibilità del corrispondente input/storico osservabile | No |
| `missing_historical_route_delay` | int | Disponibilità del corrispondente input/storico osservabile | No |
| `missing_historical_stop_delay` | int | Disponibilità del corrispondente input/storico osservabile | No |
| `missing_alert_snapshot` | int | Disponibilità del corrispondente input/storico osservabile | No |
| `route_id` | string | Metadati Silver di linea/statici | No |
| `route_type` | int | Metadati Silver di linea/statici | No |
| `direction_id` | int | Metadati Silver di linea/statici | No |
| `agency_id` | string | Metadati Silver di linea/statici | No |
| `current_status` | string | Stato causale del veicolo prima o all'istante del feed; anzianità ≤180s | No |

Medie/conteggi storici includono tutte le osservazioni sorgente valide prima del filtro originale delle etichette di training. Le finestre escludono tutte le osservazioni alla stessa epoca. Calcolare lo storico solo sull'ultima snapshot cambierebbe gli input ed è intenzionalmente evitato. Le operazioni lead statiche leggono orari/coordinate o identità della fermata successiva nel feed corrente; non leggono osservazioni future del ritardo. Restano invariati i limiti residui: orari retrospettivi a monte, timestamp feed rispetto alla disponibilità effettiva in ingestion ed esiti proxy GTFS-RT.

## Persistenza e lineage

Si tratta di input/staging per lo scoring corrente, non di storico delle prediction. L'output validato e non vuoto sovrascrive solo `next_stop_delay_scoring`, con overwrite dello schema coerente con le convenzioni del repository. `ALLOW_STAGING_OVERWRITE=False` è il gate operatore predefinito; la preparazione può essere verificata prima di abilitare una scrittura autorizzata separatamente. Non viene scritta nessun'altra tabella. I quattro campi di lineage aggiunti sono `source_silver_version` (BIGINT), `feature_prepared_at` (TIMESTAMP), `feature_preparation_batch_id` (STRING) e `feature_contract_hash` (STRING); epoca e datetime del feed identificano già il tempo dell'osservazione. Il fuso orario di sessione è Europe/Rome; l'istante di preparazione acquisito è UTC con fuso orario esplicito.

Il writer valida prima dell'overwrite e rilegge conteggio/schema/coerenza delle chiavi. Non è una transazione tra sorgente, staging e future prediction. Writer concorrenti di preparazione non sono supportati; coordinare una sola run autorizzata. Non viene introdotto storico delle prediction.

## Ambito della validazione e procedura di rollout

I test locali eseguono le definizioni di produzione e una copia fissata delle espressioni 08 precedenti al refactoring su piccoli DataFrame deterministici. Confrontano tutti i valori di preparazione training, tipi e conteggi, controllano il filtro originale di ammissibilità e verificano indipendentemente risultati noti di storico/lag. Test aggiuntivi coprono contratto/ordine/hash esatti, identificatori correnti/successivi, ammissibilità con ritardo successivo NULL, assenza di etichette/esiti nello staging, esclusione di righe future, errori di schema e controlli causali. Il valutatore di test è intenzionalmente limitato a queste espressioni; non è Spark né un sostituto dell'integrazione Spark/Delta. Le epoche delle fixture usano l'offset di gennaio di Roma UTC+1 e non validano il comportamento dell'ora legale. Non è stata installata alcuna dipendenza Spark/Java.

La seguente procedura è stata seguita nel rollout autorizzato. Conservare i passaggi per future ridistribuzioni; non implicano nuova autorizzazione runtime:

1. Registrare versioni sorgente/output e salvare una copia del notebook runtime 08. Distribuire il notebook di definizioni condivise insieme ai due chiamanti; il nome nel workspace deve essere `08_common_feature_preparation` per risolvere il `%run` relativo. Non eseguire 09–12.
2. Leggere una versione Silver fissata e confrontare in memoria preparazione 08 vecchia/nuova sullo stesso insieme deterministico di snapshot complete di corse e storico passato. Confrontare tutte le colonne originali di output, tipi, conteggi e chiavi prima di qualsiasi overwrite delle feature di training. Non ridurre lo storico prima di derivare gli input storici. Nessuna differenza inspiegata è accettabile.
3. Eseguire la preparazione corrente con scritture staging disabilitate; verificare ordine/hash esatti delle 47 feature, epoca/versione sorgente, ammissibilità delle righe, valori mancanti e identità correnti/della fermata successiva. Richiedere solo identificatori noti, non ritardi o esiti futuri.
4. Se approvato, abilitare il gate staging e creare/sovrascrivere solo `rome_transport.features.next_stop_delay_scoring`. Verificare schema Delta, conteggi, chiavi, assenza di colonne future/esiti e lineage. Conservare la vecchia versione staging se si sostituisce una tabella esistente.
5. Non eseguire training, scoring, registrazione, Gold o dashboard. Non collegare Job/pianificazione nell'ambito di questa procedura storica. Il notebook 12 rappresenta replay etichettato; l'inference operativa indipendente appartiene al futuro backend esterno.


## Risultati della validazione runtime controllata

### Notebook distribuiti e baseline delle sorgenti

La directory runtime è la cartella esistente dei notebook del progetto. Tutte e tre le sorgenti distribuite sono state riesportate e coincidevano esattamente con main, incluso il gate staging disabilitato per default. Il runtime 08 originale è stato esportato prima della sostituzione; il suo AST eseguibile coincideva con la baseline main precedente a PR3. I runtime 09–12 differivano da main solo nei commenti e sono stati preservati byte per byte. Non sono stati eseguiti.

| Notebook | Azione |
| --- | --- |
| `08_common_feature_preparation` | Creato da main; `%run` relativo verificato in Spark reale |
| `08_feature_engineering` | Aggiornato da main; notebook completo con scritture non eseguito |
| `08_current_scoring_dataset` | Creato da main; corpo effettivo validato con scritture disabilitate e Silver fissata |

Un notebook temporaneo di validazione ha svolto due esecuzioni non pianificate limitate alle feature ed è stato eliminato dopo il successo. Sorgente, export originale 08, metadati di oggetto/aggiornamento e report strutturati sono stati conservati nel pacchetto locale di evidenze dell'operatore. Nessun Job di produzione o pianificazione modificato.

Sorgente fissata: `rome_transport.silver.realtime_enriched_observations` **v8**, 15,402,700 righe; intervallo feed **1790334480–1790928849**, date di servizio **2026-09-25–2026-10-02**. Silver è rimasta v8, le feature storiche v8 e il dataset etichettato v12 prima/dopo. Nessuna tabella di feature o etichette sovrascritta.

### Parità della preparazione training vecchia/nuova

La run di validazione in sola lettura 84499547661340 (evidenze runtime conservate privatamente) si è conclusa con validazione PASS. Le espressioni legacy provenivano dal runtime 08 effettivamente esportato; nuovo helper e adapter effettivo di training da main sono stati eseguiti solo in memoria.

L'ambito deterministico usava le linee **719, 314 e 46**, mantenendo **504,787 osservazioni storiche su 919 epoche**. I confronti usavano le epoche **1790334480, 1790630629, 1790927629 e 1790928849**. Dopo il filtro originale di ammissibilità training: **2,648 righe vecchie = 2,648 righe nuove**, **105 colonne proiettate**, schemi Spark identici inclusi metadati/nullabilità e **zero differenze EXCEPT ALL in entrambe le direzioni**. Sono inclusi chiavi, valori delle feature e campi audit provvisori.

Le epoche confrontate coprivano 76 corse e 63 veicoli, con 1,086 osservazioni senza posizione veicolo, 1,675 con posizione, 31 ritardi correnti NULL, 2,185 snapshot alert note e 576 sconosciute. Nessuna sequenza sparsa di fermate successive compariva in questo specifico ambito di parità; il controllo separato sulla snapshot corrente sotto riportato ha trovato 55 transizioni sparse reali. Il risultato è una validazione rappresentativa dell'ambito selezionato, non un confronto esaustivo dell'intera tabella storica delle feature.

I controlli di provenienza storica sono passati in Spark. Join indipendenti strettamente sul passato, su 20 righe reali deterministiche, hanno verificato gli storici linea, fermata, linea-fermata e linea-ora: conteggi esatti e medie corrispondenti entro 1e-9. Ogni epoca massima contribuente era strettamente precedente a quella corrente; le osservazioni della stessa epoca erano escluse. Anche la preparazione completa dello scoring ha superato la validazione causale condivisa nel contesto della sorgente fissata.

### Staging degli input correnti e persistenza

Il corpo effettivo del notebook di scoring è stato eseguito su Silver v8 fissata con `ALLOW_STAGING_OVERWRITE=False`; ha generato dopo la validazione solo l'errore atteso del gate di scrittura disabilitato. Dopo l'accettazione di tutti i risultati in sola lettura, la run di scrittura/rilettura 1103286384923896 (evidenze runtime conservate privatamente) ha abilitato esplicitamente il gate nell'esecuzione isolata ed effettuato una sola scrittura controllata. Il valore predefinito del notebook distribuito resta False.

Lo staging non esisteva prima del rollout. Contiene ora **24,797 righe**, **300 linee**, **1,313 corse**, **1,312 veicoli** e **6,740 fermate osservate**, tutte all'epoca **1790928849**. Un controllo indipendente con finestre SQL ha riprodotto il conteggio ammissibile, incluse **236 righe la cui fermata successiva rappresentata non aveva una stima del ritardo in arrivo** e **55 transizioni con sequenza sparsa**. Non era quindi richiesto alcun ritardo alla fermata successiva o esito futuro.

Duplicati alla granularità prevista: **0**. Ordinamento target non valido: **0**. ID dell'osservazione corrente, ID della fermata successiva e stato fisico del veicolo sono stati ispezionati separatamente. Per esempio, un'osservazione alla sequenza 9 aveva come target la sequenza 10 mentre il veicolo fisico restava alla sequenza 8; le identità non sono state confuse. Gli ID fermata ripetuti restano regolati da sequenze crescenti, non da una disuguaglianza generalizzata e non valida degli ID fermata.

Ordine grezzo del modello, nomi e tipi Spark compatibili hanno superato i controlli: **47 = 42 numeriche + 5 categoriche**, con hash originale invariato. `trip_progress_ratio`, target di ritardo, esiti etichettati/provvisori, colonne future e output predittivi sono assenti dallo staging persistito. Sono presenti come metadati solo i cinque campi documentati di identità target.

Versione Delta staging: **0**, operazione **CREATE OR REPLACE TABLE AS SELECT**, **24,797 righe di output**. DataFrame preparato e rilettura avevano ordine/nomi/tipi delle colonne e conteggi identici, con **zero differenze EXCEPT ALL sulle righe complete in entrambe le direzioni**. Delta ha rilassato i metadati nullable di cinque indicatori di valori mancanti e quattro campi di lineage; il rilassamento dello storage è stato registrato esplicitamente, senza trattarlo come variazione dei valori o dei tipi dello schema. Identità richieste e valori di lineage sono stati verificati separatamente.

Lineage persistita:

- `source_silver_version`: **8**
- `feature_preparation_batch_id`: **dd58c7ab-88f8-46f9-80fd-d07b9db8feae**
- `feature_prepared_at`: **2026-10-25T17:49:47.563874Z**
- `feature_contract_hash`: **11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5**

I controlli SQL dopo la scrittura hanno confermato una sola versione sorgente, un solo hash, un solo batch di preparazione, l'unica epoca feed, zero chiavi duplicate e ordinamento target valido. Lo staging è input corrente solo in overwrite, non storico in append né tabella di prediction o valutazione.

### Valori mancanti e fuso orario

Tutti i 47 input sono stati profilati prima e dopo la persistenza. I profili coincidevano esattamente. Risultavano **zero valori NaN e infiniti** e **nessuna feature del modello completamente NULL/inutilizzabile**. Quote NULL elevate sono limiti espliciti dell'input disponibile, non valori appena imputati. Le categorie nullable sono lasciate alla gestione dei valori invalidi dell'indexer congelato.

| Feature grezza | Righe NULL | NULL % |
| --- | ---: | ---: |
| `feed_hour` | 0 | 0.00000 |
| `feed_minute` | 0 | 0.00000 |
| `day_of_week` | 0 | 0.00000 |
| `is_weekend` | 0 | 0.00000 |
| `minutes_since_midnight` | 0 | 0.00000 |
| `current_arrival_delay_seconds` | 467 | 1.88329 |
| `current_departure_delay_seconds` | 18,688 | 75.36396 |
| `abs_current_delay_seconds` | 467 | 1.88329 |
| `is_currently_delayed_flag` | 467 | 1.88329 |
| `is_currently_early_flag` | 467 | 1.88329 |
| `lag_1_arrival_delay_seconds` | 1,779 | 7.17425 |
| `lag_2_arrival_delay_seconds` | 3,055 | 12.32004 |
| `lag_3_arrival_delay_seconds` | 4,299 | 17.33677 |
| `rolling_mean_delay_last_3_stops` | 414 | 1.66956 |
| `rolling_max_delay_last_3_stops` | 414 | 1.66956 |
| `rolling_std_delay_last_3_stops` | 1,732 | 6.98472 |
| `delay_change_from_previous_stop` | 1,832 | 7.38799 |
| `delay_acceleration` | 3,155 | 12.72331 |
| `scheduled_seconds_to_next_stop` | 18,756 | 75.63818 |
| `distance_to_next_stop_meters` | 18,756 | 75.63818 |
| `vehicle_speed` | 19,311 | 77.87636 |
| `vehicle_bearing` | 19,580 | 78.96116 |
| `vehicle_odometer` | 2,587 | 10.43271 |
| `current_stop_sequence` | 24 | 0.09679 |
| `vehicle_position_staleness_seconds` | 24 | 0.09679 |
| `vehicle_position_available_flag` | 0 | 0.00000 |
| `active_alert_flag` | 0 | 0.00000 |
| `active_alert_count` | 0 | 0.00000 |
| `active_detour_flag` | 0 | 0.00000 |
| `active_construction_flag` | 0 | 0.00000 |
| `historical_route_mean_delay` | 0 | 0.00000 |
| `historical_stop_mean_delay` | 1 | 0.00403 |
| `historical_route_stop_mean_delay` | 2 | 0.00807 |
| `historical_route_hour_mean_delay` | 0 | 0.00000 |
| `historical_route_observation_count` | 0 | 0.00000 |
| `historical_stop_observation_count` | 0 | 0.00000 |
| `missing_vehicle_position` | 0 | 0.00000 |
| `missing_current_arrival_delay` | 0 | 0.00000 |
| `missing_current_departure_delay` | 0 | 0.00000 |
| `missing_historical_route_delay` | 0 | 0.00000 |
| `missing_historical_stop_delay` | 0 | 0.00000 |
| `missing_alert_snapshot` | 0 | 0.00000 |
| `route_id` | 0 | 0.00000 |
| `route_type` | 18,751 | 75.61802 |
| `direction_id` | 0 | 0.00000 |
| `agency_id` | 18,751 | 75.61802 |
| `current_status` | 24 | 0.09679 |

Limiti rilevanti: ritardo in partenza circa 75.36% NULL; intervallo programmato/distanza circa 75.64%; velocità/orientamento circa 77.88%/78.96%; route_type/agency_id circa 75.62%. Sono input nullable compatibili con il preprocessing congelato, ma questo rollout non dimostra la qualità del modello con tale distribuzione di valori mancanti.

Il fuso orario della sessione Spark era esplicitamente **Europe/Rome**. Le righe reali all'epoca 1790928849 coincidevano con la conversione del fuso orario Python: **2026-10-02T10:14:09+02:00**, feed_hour=10, feed_minute=14, minutes_since_midnight=614, day_of_week=6 (venerdì), is_weekend=0. È una data non ambigua precedente al cambio dell'ora legale. Il fuso precedente **Etc/UTC** è stato ripristinato in entrambe le run isolate; nessuna impostazione permanente del fuso orario cambiata.

### Confini, rollback e readiness

Non è servito rollback. Sorgente runtime 08 originale e metadati di aggiornamento restano nel pacchetto di evidenze; helper/scoring erano nuovi oggetti. Lo staging era nuovo, senza versione precedente da ripristinare. Un futuro rollback deve preservare le evidenze ed evitare di toccare Silver o tabelle storiche di feature/etichette; nessun DROP o RESTORE automatico è stato eseguito.

Nessun modello è stato caricato, addestrato o usato per transform durante PR3. Nessuna run MLflow o registrazione creata/modificata e notebook 09–13 non eseguiti. Gold, dashboard, Power BI, impostazioni ingestion e repository dell'app non sono stati toccati. Il contratto degli input validato è riusato per l'handoff operativo esterno; freshness della sorgente e incidenza elevata di valori mancanti restano limiti. Questo risultato non dichiara un servizio operativo corrente o qualità delle prediction.

## Divario di freshness downstream

L'inventario in sola lettura del 25 ottobre 2026 conferma un solo Job pianificato con un task: notebook 05, ogni 10 minuti. Non invoca 06, 07 o la preparazione degli input correnti di scoring. Bronze Trip Updates è avanzata all'epoca 1792960055; osservazioni Silver/enrichment e staging restano a 1790928849. Il gate overwrite staging resta OFF per default. Non è configurato alcun refresh downstream automatico.

Il prossimo rollout autorizzato separatamente deve implementare la catena di dipendenze con versioni fissate 05 -> 06 -> 07 -> `08_current_scoring_dataset`, misurare i costi sull'intero storico, riconciliare comportamento di duplicati/sorgenti/fuso orario e pubblicare un manifest di readiness validato prima di inference esterna. Non pianificare etichettatura/training storici a questa cadenza. Vedi [analisi della freshness e accettazione dell'orchestrazione](OPERATIONAL_ML_HANDOFF.md#divario-di-freshness-e-disegno-dellorchestrazione-interna).
