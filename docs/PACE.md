# Rome Public Transport Reliability & Delay Prediction

## Progetto end-to-end di Machine Learning e analisi dei dati

**Stack tecnologico:** Databricks · Apache Spark / PySpark · Delta Lake · Unity Catalog · MLflow · Databricks SQL · Databricks AI/BI Dashboards · Python · SQL

**Tipologia:** progetto di portfolio in Data Analytics e Machine Learning con approfondimento tecnico di livello senior

**Ambito geografico:** Roma, Italia

**Fonte primaria:** Roma Servizi per la Mobilità

---

## Quadro dell'implementazione — 7 ottobre 2026

Il documento mantiene il framework PACE e le motivazioni progettuali, distinguendo ipotesi iniziali, decisioni implementate, risultati verificati ed estensioni future. I risultati riportati provengono dalle esecuzioni validate comunicate per il progetto; questa revisione documentale non riesegue i Job Databricks.

**Completati:** ingestion pianificata, Bronze/Silver, feature progettate per prevenire il leakage, etichettatura futura e filtro di qualità, tracciamento MLflow, selezione su validation, valutazione TEST congelata, batch scoring con impostazione assimilabile alla produzione, otto tabelle Gold, dashboard Databricks AI/BI su cinque pagine ed export JSON/PDF per il portfolio.

**Scostamenti dal piano iniziale:** Persistence è la soluzione finale di regressione, perché la maggiore complessità non ha prodotto un miglioramento sufficiente in validazione. La raccolta avviene ogni 10 minuti, anziché ogni cinque come proposto inizialmente. Gold utilizza tabelle analitiche compatte per snapshot, al posto dei nomi dimensionali ipotizzati.

**Rinviata:** registrazione del modello in produzione, a causa del limite di serializzazione degli artefatti SparkML / Unity Catalog nell'ambiente Serverless corrente. Scoring e tracciamento degli esperimenti sono completati indipendentemente dalla registrazione.

**Riconciliazione metodologica da completare:** il codice di etichettatura e Gold definisce il ritardo grave come >=300 secondi; il testo della dashboard usa >600 secondi. I risultati rimangono invariati e non sono reinterpretati come risultati validati per >600 secondi. Differiscono anche le fasce di ritardo e probabilità. La sezione 14 esplicita entrambe le definizioni; codice e JSON della dashboard non vengono modificati.

**Distribuzione:** la dashboard pubblicata richiede autenticazione Databricks. L'export JSON e il PDF completo sono presenti nel repository. Una presentazione Power BI pubblica separata rimane futura. Il vincolo di accesso riguarda la distribuzione della dashboard interattiva, non il funzionamento dell'implementazione Databricks.

# 1. PANORAMICA DEL PROGETTO

Rome Public Transport Reliability & Delay Prediction è un progetto end-to-end di analisi e Machine Learning dedicato all'affidabilità del trasporto pubblico di Roma e alla previsione dei ritardi tramite dati GTFS programmati e realtime.

L'implementazione acquisisce periodicamente la telemetria del trasporto pubblico, la trasforma in dataset analitici storici, valuta modelli predittivi e presenta informazioni curate tramite Databricks AI/BI Dashboards. È un progetto di portfolio con impostazione assimilabile alla produzione, non un'API live o una dichiarazione di adozione operativa.

Il progetto integra quattro capacità complementari:

1. Data Engineering e ingestion;
2. analisi operativa;
3. Machine Learning;
4. Business Intelligence.

La domanda centrale è:

> **Le condizioni correnti del servizio e il comportamento storico consentono di prevedere i ritardi successivi e individuare linee, fermate e periodi con il maggiore rischio di inaffidabilità?**

L'architettura implementata collega dati, analisi, modelli e BI:

```text
Open data di Roma Mobilità
        ↓
Ingestion Databricks
        ↓
Archivio storico Bronze / Silver
        ↓
Feature Engineering
        ↓
Etichettatura degli esiti futuri
        ↓
Machine Learning / tracciamento MLflow
        ↓
batch scoring
        ↓
Livello Gold
        ↓
Databricks SQL
        ↓
Databricks AI/BI Dashboards
```

---

# 2. FRAMEWORK PACE

Il percorso è organizzato nelle quattro fasi PACE: pianificazione, analisi, costruzione ed esecuzione. Le ipotesi progettuali restano riconoscibili rispetto alle scelte effettivamente implementate.

---

# P — PIANIFICAZIONE

## 2.1 Obiettivo del progetto

L'affidabilità del trasporto pubblico varia in funzione di linea, fermata, direzione, ora, giorno della settimana, traffico, ritardi precedenti, interruzioni e condizioni operative.

I feed GTFS e GTFS-Realtime contengono informazioni dettagliate, ma non sono direttamente leggibili da un interlocutore non tecnico. Il progetto le trasforma in dati curati sull'affidabilità e sul rischio previsto, articolando tre livelli di analisi.

### Analisi descrittiva

**Che cosa sta accadendo?**

- Quali linee accumulano più ritardi?
- Quali fermate presentano i ritardi medi maggiori?
- In quali ore i ritardi sono più frequenti?
- Come varia l'affidabilità per linea, giorno e fascia oraria?

### Analisi diagnostica

**Dove emergono i ritardi e quali associazioni si osservano?**

- Il ritardo si propaga dalle fermate precedenti?
- Esistono segmenti di linea ricorrentemente problematici?
- Alcune linee peggiorano in fasce orarie specifiche?
- Gli avvisi di servizio sono associati a un aumento dei ritardi?

Queste domande orientano l'analisi; le associazioni osservate non costituiscono prove causali.

### Analisi predittiva

**Che cosa è probabile che accada alla fermata successiva?**

> Prevedere il ritardo atteso e il rischio di ritardo grave di un veicolo alla prossima fermata.

---

# 3. OBIETTIVI DI BUSINESS

Il sistema analitico affronta i seguenti obiettivi:

1. monitorare l'affidabilità del trasporto pubblico;
2. individuare ricorrenze nei ritardi;
3. misurare le prestazioni di linee e fermate;
4. costruire profili operativi storici;
5. prevedere ritardi a breve termine;
6. individuare linee e fermate con rischio operativo elevato;
7. confrontare previsioni ed esiti osservati;
8. rendere disponibili i risultati tramite una dashboard direzionale in Databricks AI/BI Dashboards.

---

# 4. INTERLOCUTORI E FABBISOGNI INFORMATIVI

La progettazione rappresenta le esigenze dei seguenti interlocutori, senza implicare che abbiano adottato il progetto operativamente.

## Responsabile delle operazioni

Deve comprendere quali linee stanno peggiorando, dove si accumulano ritardi e quali aree richiedono attenzione.

## Analista della pianificazione di rete

Ha bisogno di individuare colli di bottiglia ricorrenti, linee poco affidabili, differenze per giorno e fascia oraria, distinguendo problemi sistematici e temporanei.

## Analista Data / BI

Richiede tabelle analitiche affidabili, definizioni coerenti dei KPI, trasformazioni riproducibili e modelli semantici verificabili.

## Gruppo Data Science / ML

Richiede osservazioni storiche etichettate, dataset di feature riutilizzabili, tracciamento degli esperimenti, monitoraggio dei modelli e training riproducibile.

---

# 5. PERIMETRO DEL PROGETTO

## Componenti incluse

La pipeline implementata utilizza le seguenti sorgenti, nei limiti della disponibilità effettiva.

### GTFS Static

```text
agency
routes
trips
stops
stop_times
calendar
calendar_dates
shapes
```

### GTFS-Realtime

```text
Trip Updates
Vehicle Positions
Service Alerts
```

I feed realtime sono raccolti periodicamente e salvati in tabelle Delta per costruire il dataset storico.

---

# 6. COMPONENTI ESCLUSE DALLA VERSIONE 1

Restano fuori dall'implementazione completata:

- previsione della domanda di passeggeri;
- dati di bigliettazione e comportamento dei singoli passeggeri;
- valutazione dei conducenti;
- ottimizzazione dell'assegnazione dei veicoli e riprogettazione delle linee;
- modelli di deep learning e reinforcement learning;
- API live di produzione e applicazioni mobili in tempo reale;
- integrazione dettagliata di sensori del traffico, meteo e calendari di eventi.

Questi elementi possono diventare estensioni successive. La delimitazione del perimetro evita complessità non necessaria prima di aver stabilito baseline affidabili.

---

# 7. FONTI DEI DATI

## 7.1 Roma Mobilità — GTFS Static

È la fonte primaria per la rete di trasporto programmata: linee, corse, fermate, sequenza delle fermate, arrivi e partenze programmati, calendario, servizio, direzione e tracciato.

Il dataset statico costituisce il modello di riferimento della rete. Gli identificatori GTFS sono mantenuti nelle trasformazioni per collegare programmazione e osservazioni realtime.

---

# 8. GTFS-REALTIME — TRIP UPDATES

È la fonte principale delle informazioni operative sui ritardi. I campi potenzialmente disponibili includono:

```text
trip_id
route_id
start_time
start_date
vehicle_id
stop_id
stop_sequence
arrival_delay
departure_delay
arrival_time
departure_time
timestamp
```

L'ingestion verifica i campi disponibili. I campi opzionali GTFS-Realtime possono mancare e non vengono considerati universalmente popolati.

---

# 9. GTFS-REALTIME — VEHICLE POSITIONS

Fornisce osservazioni spaziali sui veicoli in servizio. Tra i campi previsti:

```text
vehicle_id
trip_id
route_id
latitude
longitude
bearing
speed
current_stop_sequence
current_status
timestamp
```

L'associazione utilizza `vehicle_id` e `trip_id` con matching causal backward as-of. Sono ammesse esclusivamente posizioni con `vehicle_timestamp <= feed_timestamp` e anzianità massima di 180 secondi; viene scelta la più recente tra quelle valide. Le posizioni future non possono contribuire alle feature disponibili al momento della previsione.

---

# 10. AVVISI DI SERVIZIO

I Service Alerts sono acquisiti separatamente e aggiunti al contesto delle osservazioni. Possono fornire linea, fermata, effetto, causa, descrizione, `start_time` ed `end_time`.

La progettazione iniziale considerava questi campi concettuali:

```text
active_service_alert
route_disruption_flag
stop_disruption_flag
```

Il contratto implementato utilizza campi come `active_alert_flag` e `active_alert_count`. I nomi concettuali non implicano l'esistenza di ulteriori colonne finali.

---

# 11. STRATEGIA DI RACCOLTA

I feed GTFS-Realtime rappresentano lo stato operativo corrente; non offrono direttamente un dataset storico pronto per l'analisi. La costruzione dell'archivio di snapshot è quindi una componente centrale del lavoro di Data Engineering.

```text
Endpoint GTFS-Realtime
       ↓
Ingestion pianificata
       ↓
Protobuf grezzo
       ↓
Decodifica
       ↓
Associazione del timestamp alla snapshot
       ↓
Scrittura in append su Delta
```

La frequenza implementata è **ogni 10 minuti**, tramite un Job Databricks. La proposta iniziale di cinque minuti è stata superata. Frequenza di aggiornamento della sorgente e frequenza di acquisizione sono distinte: l'archivio non garantisce la cattura di ogni aggiornamento del feed.

`rome_transport.bronze.ingestion_runs` registra stato ed errori, inclusi i problemi transitori delle sorgenti.

---

# 12. CONSERVAZIONE E TRACCIABILITÀ

La progettazione iniziale dell'audit prevedeva i seguenti campi; i notebook di ingestion definiscono quelli effettivamente implementati:

```text
ingestion_timestamp
source_timestamp
source_file
pipeline_run_id
```

Queste informazioni consentono ricostruzione storica e verifiche. Lo storico realtime viene accumulato aggiungendo snapshot; l'acquisizione delle sorgenti statiche e gli aggiornamenti analitici successivi hanno strategie di scrittura proprie. Non tutte le tabelle sono quindi gestite esclusivamente in append.

---

# 13. CRITERI DI SUCCESSO

I criteri progettuali sono mantenuti come riferimento di valutazione; risultati e scostamenti sono documentati nelle sezioni 36, 48 e 52.

### Data Engineering

- Ingestion GTFS Static riproducibile e decodifica automatica GTFS-Realtime.
- Persistenza delle snapshot storiche.
- Trasformazioni Bronze → Silver riproducibili.
- Gestione dei duplicati e controlli di qualità dei dati.

### Analisi

Calcolo di ritardo medio, mediano e P90, quote di regolarità e ritardo grave, affidabilità per linea/fermata/ora e propagazione del ritardo.

### Machine Learning

Un modello di regressione più complesso deve superare una baseline significativa per giustificarne la sostituzione. Persistence è rimasta la soluzione più forte in validazione: mantenerla è coerente con una selezione fondata sulle evidenze.

Un esempio di baseline alternativa è il ritardo medio storico per linea × fermata × ora. Il classificatore deve essere confrontato con riferimenti significativi, come la classe maggioritaria o la frequenza storica di ritardo grave.

Entrambi i task richiedono valutazione su osservazioni temporalmente successive e non utilizzate nella selezione, prevenzione del leakage e metriche specifiche del problema.

### Business Intelligence

La dashboard deve rispondere a domande operative senza richiedere l'apertura dei notebook tecnici. I dataset devono provenire esclusivamente da Gold, senza collegamenti diretti a Bronze o Silver.

---

# 14. KPI E DEFINIZIONI OPERATIVE

Le statistiche di ritardo sono espresse in secondi, salvo conversione esplicita in minuti. Le quote sono frazioni in [0,1] e sono ponderate per osservazione, non per passeggero o corsa unica. Snapshot ripetute possono generare più righe per la stessa corsa e fermata.

## KPI statistici implementati

Media, mediana, P90, P95 e P99 del ritardo realizzato alla fermata successiva; quote di regolarità e delle fasce di ritardo; affidabilità per linea, fermata e tempo; rischio alla fermata successiva; metriche di valutazione congelate. I percentili approssimati usano Spark `percentile_approx` con accuratezza 10,000. Un indice composito di affidabilità rimane un'idea opzionale, non un KPI finale pubblicato.

## Fasce operative finali della dashboard

| Stato | Ritardo in secondi |
| --- | --- |
| In Anticipo | delay < -60 |
| Regolare | -60 <= delay <= 300 |
| Ritardo Moderato | 300 < delay <= 600 |
| Ritardo Grave | delay > 600 |

## Target ML implementato e differenza da riconciliare

Il classificatore è stato addestrato e valutato sul target ML `target_realized_major_delay_flag`, calcolato dal notebook 09 come **ritardo realizzato alla fermata successiva >=300 secondi**. La dashboard adotta invece **Ritardo Grave >600 secondi** come fascia di severità operativa: una tassonomia di presentazione dello stato del servizio, distinta dalla definizione dell'evento appreso dal modello. Le due soglie hanno finalità analitiche diverse e non sono intercambiabili. Il notebook 13 valida questa definizione e usa le fasce: anticipo < -60, regolarità [-60,180), ritardo moderato [180,300), ritardo grave >=300. Il testo della dashboard descrive invece l'evento >600 secondi.

Le query SQL dell'export leggono campi Gold già calcolati: le etichette di presentazione non ne ricalcolano il significato. Le metriche del classificatore non diventano quindi metriche per >600 secondi per effetto di una modifica testuale.

Anche le fasce di rischio differiscono: il notebook 13 usa p<0.20, 0.20<=p<=0.45, 0.45<p<0.80 e p>=0.80; la presentazione finale usa Basso p<=0.30, Medio 0.30<p<=0.50, Alto 0.50<p<=0.70 e Critico p>0.70. La decisione binaria del classificatore rimane **strettamente p>0.45**, distinta dalle fasce visuali.

La riconciliazione di etichette, Gold e dashboard è un **miglioramento metodologico futuro**. Conteggi, quote e metriche sono preservati, ma non possono essere reinterpretati come risultati validati per >600 secondi sulla base degli artefatti disponibili. La probabilità si riferisce sempre all'evento alla fermata successiva definito dal target usato nel training. Questo limite semantico è distinto dai controlli strutturali Gold superati.

---

# 15. OBIETTIVI DI MACHINE LEARNING

## Task principale: regressione

Target implementato: `target_realized_next_stop_delay_seconds` in `rome_transport.features.next_stop_delay_labeled`.

La prima snapshot successiva valida della stessa corsa, data di servizio e fermata target identificata da ID/sequenza, entro 30 minuti, fornisce il proxy dell'esito realizzato. Si privilegia il ritardo in arrivo; quello in partenza è un'alternativa quando disponibile. Le osservazioni senza esito futuro rimangono prive di etichetta, non vengono convertite in zero.

## Task secondario: classificazione binaria

Target implementato localmente: `target_realized_major_delay_flag`, derivato dall'etichetta realizzata di regressione con soglia >=300 secondi. I nomi generici proposti inizialmente sono stati superati. La soglia >600 secondi della dashboard è documentata separatamente nella sezione 14 e non viene sostituita retroattivamente nel classificatore valutato.

La stessa definizione di etichetta vale per train / validation / test. La soglia di probabilità è stata selezionata su validation, congelata a >0.45 in senso stretto e non ritoccata sul TEST.

## Provenienza e interpretazione delle etichette

Timestamp futuro, corsa, data di servizio, fermata e orizzonte consentono l'audit. `provisional_next_stop_delay_seconds` è conservato dalla stessa snapshot solo per diagnostica, mai come esito finale o predittore. Le osservazioni future GTFS-Realtime rimangono proxy operativi e possono essere previsioni riviste; informazioni sullo stato del veicolo e controlli di qualità aiutano a descrivere questo limite.

---

# 16. UNITÀ DI PREVISIONE

La granularità esatta è `feed_timestamp, entity_id, trip_id, stop_sequence, stop_id`. Il contesto di business corrisponde approssimativamente a veicolo × corsa × fermata × timestamp.

Esempio illustrativo, non un risultato finale misurato:

```text
route_id: 64
trip_id: 123456
vehicle_id: 8172
current_stop: Termini
timestamp: 08:42

current_delay: 4.3 min
previous_delay: 3.1 min

TARGET
Ritardo alla fermata successiva: 5.6 min
```

---

# 17. ORIZZONTE DI PREVISIONE

L'orizzonte implementato è la fermata successiva prevista, ricercando osservazioni successive per l'etichetta entro 30 minuti.

Estensioni possibili: +2 fermate, +3 fermate, +10 minuti o +20 minuti. La prima versione privilegia l'orizzonte più breve perché offre un'etichetta più chiara e una relazione operativa più diretta.

---

# 18. MODELLI DI BASELINE

Le prestazioni ML devono essere confrontate con alternative semplici.

## Baseline di regressione

### Baseline 1: Persistence

```text
next_delay = current_delay
```

Verifica quanto il ritardo corrente persista alla fermata successiva. È diventata la soluzione finale: `current_arrival_delay_seconds` con valore sostitutivo pari alla mediana del train, -71 secondi. Le alternative storiche, lineari e ad alberi non hanno fornito un miglioramento sufficiente in validazione.

### Baseline 2: media storica

La proposta iniziale considerava la media per linea × fermata × giorno della settimana × ora.

### Baseline 3: mediana storica

Un'altra alternativa progettuale era la mediana per la stessa linea e fermata.

## Baseline di classificazione

- Classe maggioritaria.
- Frequenza storica di ritardo grave per linea/fermata/ora.
- Soglia semplice su `current_delay`, quando metodologicamente appropriata e disponibile alla previsione.

Statistiche storiche e classe maggioritaria devono essere stimate solo su osservazioni passate ammissibili del train. Le baseline condividono la definizione del target dei classificatori. Il confronto con la classe maggioritaria è implementato; le altre regole sono alternative progettuali, non ulteriori risultati finali dichiarati.

La maggiore complessità è giustificata solo da un miglioramento validato rispetto a questi riferimenti.

---

# A — ANALISI

# 19. PROFILAZIONE DEI DATI

Profilazione e diagnostica implementate esaminano numero di righe, copertura per data, linea, fermata e veicolo, valori mancanti, duplicati, coerenza dei timestamp, distribuzione dei ritardi, valori estremi e frequenza degli aggiornamenti.

Queste verifiche collegano la qualità del feed alla copertura effettivamente utilizzabile per analisi e training, evitando di confondere volume acquisito e dataset etichettato finale.

---

# 20. INDAGINE SULLA QUALITÀ DEI DATI

Le principali domande diagnostiche sono:

- Ogni veicolo è associato a una corsa valida?
- Gli ID delle corse sono presenti nei dati GTFS Static?
- Le fermate sono identificate coerentemente?
- I campi di ritardo sono sempre popolati?
- I feed ripetono talvolta snapshot identiche?
- I timestamp sono generati dalla sorgente o dall'ingestion?
- Con quale frequenza i veicoli risultano assenti dal feed?
- I ritardi negativi rappresentano anticipi plausibili?
- Alcune linee hanno una copertura realtime sistematicamente inferiore?

L'assenza di un'osservazione non viene interpretata automaticamente come assenza di ritardo o servizio regolare.

---

# 21. ANALISI TEMPORALE

La progettazione iniziale considerava ora, giorno della settimana, fine settimana, mese, periodi di punta/fuori punta e periodo di servizio. I profili di interesse comprendevano punta mattutina, fascia centrale, punta serale e servizio notturno.

Le aggregazioni Gold implementate usano `service_date`, `day_of_week` secondo la convenzione ISO e `feed_hour` nel fuso `Europe/Rome`. Le fasce sono: Notte 00–05, Mattina 06–09, Fascia centrale 10–15, Punta serale 16–19, Sera 20–23.

Giorno della settimana e indicatore del fine settimana derivano dalla data civile del feed; `service_date` resta distinto, per preservare la gestione del giorno di servizio. Otto date di servizio non supportano conclusioni stagionali o mensili.

Quote di ritardo grave riportate: Notte **0.06551**, Mattina **0.15457**, Fascia centrale **0.18944**, Punta serale **0.20327**, Sera **0.25224**. Alle ore 20: **0.30286**. Giorni feriali: **0.18574**; fine settimana: **0.19748**. Rimane valida la precisazione sulle definizioni della sezione 14.

---

# 22. ANALISI PER LINEA

Le misure considerate per linea comprendono numero di corse e osservazioni, ritardo medio, mediano e P90, variabilità, quota di regolarità e quota di ritardo grave.

Le classifiche richiedono almeno **1,000 osservazioni**: nel dataset finale risultano ammissibili **354 linee**. I gruppi più piccoli mantengono le metriche ma sono esclusi dal ranking, così da non confrontare campioni minimi e grandi con lo stesso criterio.

Le analisi descrivono il periodo osservato; otto date di servizio non consentono di inferire persistenza sul lungo termine.

---

# 23. ANALISI PER FERMATA

La progettazione iniziale considerava osservazioni, ritardo medio/mediano/P90 in arrivo, numero di linee e variabilità del ritardo.

Le classifiche implementate richiedono almeno **500 osservazioni**, con **5,172 fermate** ammissibili. Le metriche descrivono il ritardo realizzato alla fermata successiva raggruppato per la fermata corrente: non sono misure indipendenti dell'arrivo effettivo alla fermata indicata.

I riferimenti iniziali a metriche di arrivo rappresentano quindi l'intento progettuale; l'interpretazione finale deve rispettare il target e la granularità implementati.

---

# 24. PROPAGAZIONE DEL RITARDO

L'analisi implementata raggruppa le osservazioni storiche per fascia di ritardo corrente e precedente. La sequenza seguente è solo un esempio progettuale, non una corsa misurata:

```text
Fermata A    +1 min
Fermata B    +2 min
Fermata C    +4 min
Fermata D    +7 min
```

Tra le metriche inizialmente ipotizzate:

```text
delta_delay_from_previous_stop
delay_growth_rate
cumulative_delay
```

Il contratto delle feature implementa `lag_1_arrival_delay_seconds`, `lag_2_arrival_delay_seconds`, `lag_3_arrival_delay_seconds` e `delay_change_from_previous_stop`. Le sintesi di propagazione sono descrittive, non causali.

Quando la fermata corrente è in Ritardo Grave, la frequenza di ritardo grave alla successiva è **0.88714**, su circa **1.64 milioni di osservazioni**, con mediana del ritardo successivo di **740 secondi**. La transizione da fermata precedente Regolare a corrente in Ritardo Grave presenta frequenza **0.51389**.

Le fasce registrate devono essere riconciliate con la presentazione della dashboard prima di interpretare questi risultati come transizioni riferite alla soglia >600 secondi.

---

# 25. FEATURE ENGINEERING

La progettazione iniziale dei gruppi di feature è conservata di seguito. I nomi concettuali non costituiscono lo schema completo implementato. Il notebook 08 crea la tabella effettiva; i notebook 10–12 utilizzano elenchi espliciti di colonne ammesse ed escluse.

Le feature implementate sulle fermate precedenti includono `lag_1_arrival_delay_seconds`, `lag_2_arrival_delay_seconds`, `lag_3_arrival_delay_seconds`, `rolling_mean_delay_last_3_stops`, `rolling_max_delay_last_3_stops` e `delay_change_from_previous_stop`. Le finestre da 15/30/60 minuti riportate sotto sono alternative progettuali, non feature dichiarate come già implementate.

## Feature della linea

```text
route_id
direction_id
route_type
```

## Feature della fermata

```text
stop_id
stop_sequence
```

## Feature temporali

```text
hour
minute
day_of_week
is_weekend
month
```

## Stato corrente

```text
current_delay
previous_stop_delay
current_stop_sequence
```

## Feature storiche

```text
avg_route_delay
median_route_delay
avg_route_stop_delay
avg_route_hour_delay
avg_stop_hour_delay
```

## Finestre mobili ipotizzate

```text
rolling_delay_15m
rolling_delay_30m
rolling_delay_60m
rolling_route_delay
```

## Propagazione

```text
delay_change_previous_stop
cumulative_delay
trip_progress_pct
```

## Feature del veicolo

Utilizzabili quando sufficientemente affidabili:

```text
speed
bearing
latitude
longitude
distance_to_next_stop
```

## Disservizi

```text
active_service_alert
route_alert
stop_alert
```

---

# 26. PREVENZIONE DEL DATA LEAKAGE

È un requisito metodologico centrale. Regressione e classificazione devono utilizzare solo informazioni disponibili al momento della previsione, considerando sia il timestamp della sorgente sia l'effettiva disponibilità in ingestion.

Sono ammissibili il ritardo corrente, le fermate precedenti, le medie storiche, l'ora corrente e la posizione del veicolo già disponibile. Non sono ammissibili esiti futuri, posizioni future, aggregazioni che includano dati successivi o feature calcolate con osservazioni posteriori.

Lag e finestre mobili implementati usano esclusivamente fermate precedenti nella stessa snapshot della corsa. Gli aggregati storici usano timestamp strettamente precedenti, escludendo tutte le osservazioni con lo stesso timestamp della previsione. Il matching delle posizioni è causal backward as-of con anzianità massima di 180 secondi. Gli esiti futuri della fermata successiva servono soltanto come etichette.

La causalità rispetto al tempo del feed è controllata, ma il solo timestamp della sorgente non dimostra l'esatta disponibilità al momento dell'ingestion. Target provvisori, provenienza delle etichette future e diagnostiche derivate dagli esiti sono esclusi mediante contratti espliciti delle feature.

Gli stessi controlli valgono per entrambi i task. Preprocessing ed eventuale bilanciamento delle classi devono essere stimati sul train. Se l'esito necessario a un'etichetta non era ancora disponibile al confine con la partizione successiva, la riga viene esclusa dalla partizione precedente.

---

# 27. SUDDIVISIONE TRAIN / VALIDATION / TEST

Entrambi i task utilizzano gruppi cronologici di timestamp distinti: circa **70% train, 15% validation e 15% TEST**. Non si applica uno split casuale delle righe. Gli esempi iniziali su più settimane sono stati superati dal dataset disponibile, che copre otto date di servizio.

```text
TRAIN: prime snapshot in ordine temporale
VALIDATION: snapshot successive
TEST: snapshot finali riservate alla valutazione
```

Durante la valutazione, il preprocessing viene stimato solo sul train. Si eliminano dal train le etichette che oltrepassano il confine di disponibilità della validation e dalla validation quelle che oltrepassano il confine del TEST.

Modelli e iperparametri sono selezionati su validation; tutte le decisioni finali, compreso il confronto stretto p>0.45, sono congelate prima del TEST.

Il riaddestramento del notebook 12 sull'intero storico ammissibile serve al batch scoring dopo la selezione. Non genera metriche TEST sostitutive; valutare l'ultima snapshot storica non equivale a una nuova verifica su dati mai osservati.

---

# C — COSTRUZIONE

# 28. ARCHITETTURA DATABRICKS

Il progetto implementa un'architettura Medallion. L'etichettatura futura si colloca tra preparazione delle feature e valutazione supervisionata. La registrazione non è un prerequisito per il percorso di scoring in memoria già completato.

```text
Sorgenti GTFS Static / GTFS-Realtime
        ↓
Bronze
        ↓
Silver
        ↓
Feature Engineering
        ↓
Etichettatura futura
        ↓
Modelli ML / valutazione
        ↓
Previsioni batch
        ↓
Gold
        ↓
Databricks SQL
        ↓
Databricks AI/BI Dashboards
```

Le analisi descrittive e i risultati predittivi confluiscono in Gold con percorsi distinti, evitando di introdurre etichette future negli output operativi.

---

# 29. STRUTTURA UNITY CATALOG

Namespace implementati:

```text
rome_transport
├── bronze
├── silver
├── features
├── ml
└── gold
```

Il nome previsto per la registrazione del classificatore è `rome_transport.ml.major_delay_classifier`, non il nome provvisorio iniziale `delay_prediction_model`. Non si dichiara una registrazione riuscita in produzione. Esperimenti MLflow e metadati Delta garantiscono tracciabilità mentre la registrazione degli artefatti resta rinviata; si veda la sezione 40.

---

# 30. TABELLE BRONZE

Bronze conserva informazioni grezze o minimamente trasformate. La progettazione iniziale considerava i seguenti nomi, che non costituiscono un inventario degli oggetti effettivamente persistiti:

```text
bronze.gtfs_routes
bronze.gtfs_trips
bronze.gtfs_stops
bronze.gtfs_stop_times
bronze.gtfs_calendar

bronze.trip_updates_raw
bronze.vehicle_positions_raw
bronze.service_alerts_raw
```

Tra i nomi implementati figurano `rome_transport.bronze.agency`, `rome_transport.bronze.routes` e `rome_transport.bronze.ingestion_runs`. I notebook di ingestion definiscono le altre tabelle effettive. Bronze mantiene la tracciabilità verso le sorgenti; i nomi `gtfs_*` sopra riportati sono esempi progettuali.

---

# 31. TABELLE SILVER

Silver rappresenta entità operative validate e normalizzate. La suddivisione concettuale iniziale era:

```text
silver.routes
silver.stops
silver.trips
silver.stop_schedule

silver.trip_updates
silver.vehicle_positions
silver.service_alerts

silver.trip_stop_observations
```

Le tabelle principali implementate comprendono `rome_transport.silver.service_calendar`, `trip_schedule`, `trip_stop_schedule`, `trip_updates`, `vehicle_positions`, `realtime_trip_stop_observations` e `realtime_enriched_observations`. I nomi semplificati e lo schema seguente rappresentano concetti di progetto, non ulteriori oggetti persistiti.

Schema concettuale:

```text
observation_timestamp
route_id
trip_id
vehicle_id
stop_id
stop_sequence

scheduled_arrival
predicted_arrival

arrival_delay_seconds
departure_delay_seconds

vehicle_latitude
vehicle_longitude

service_date
```

---

# 32. TABELLE DELLE FEATURE E DELLE ETICHETTE

Tabella delle feature: `rome_transport.features.next_stop_delay_features`.

Tabella etichettata finale: `rome_transport.features.next_stop_delay_labeled`.

Granularità esatta:

```text
feed_timestamp × entity_id × trip_id × stop_sequence × stop_id
```

Le tabelle mantengono anche campi di audit; nei modelli entrano soltanto le colonne ammesse esplicitamente e disponibili alla previsione. La copertura finale dopo il filtro è di **9,975,703 righe, 964 snapshot, 8 date di servizio, 412 linee, 82,936 corse e 2,157 veicoli**.

Le diagnostiche precedenti rimangono separate per fase:

- Matching causale: 15,402,700 osservazioni, 13,610,934 posizioni associate (88.37%), zero posizioni future e zero duplicati.
- Feature Engineering prima dell'etichettatura futura: 14,475,546 righe, 969 snapshot, 88,242 corse, 2,158 veicoli e 423 linee.
- Etichettatura futura prima del filtro: 10,007,020 righe, copertura 69.13% e 83,041 corse.

Questi denominatori non devono essere confusi con i conteggi finali successivi al filtro.

---

# 33. LIVELLO GOLD

Il notebook 13 implementa Gold per Databricks SQL e AI/BI Dashboards. La proposta iniziale con nomi `dim_*`/`fact_*` è stata sostituita da snapshot analitiche compatte in `rome_transport.gold`:

| Tabella | Granularità e finalità |
| --- | --- |
| executive_overview | Una riga: KPI storici e sintesi indipendente dell'ultimo scoring |
| route_reliability | route_id: confronti tra linee e ranking dei gruppi ammissibili |
| stop_reliability | stop_id: contesto della fermata corrente ed esiti alla successiva |
| temporal_reliability | service_date, day_of_week, feed_hour |
| delay_propagation | current_delay_band, previous_delay_band |
| predictive_operations | feed_timestamp, entity_id, trip_id, stop_sequence, stop_id |
| model_performance | task, dataset, metric_name |
| analytics_refresh_metadata | Una riga: versioni sorgente, provenienza e manifest dell'aggiornamento validato |

Le tabelle storiche usano etichette realizzate; `predictive_operations` usa solo previsioni di scoring ed esclude target e provenienza delle etichette. I KPI direzionali combinano sintesi aggregate separatamente, non etichette storiche e previsioni riga per riga.

Le scritture Delta usano overwrite con sostituzione dello schema. Le versioni sorgente vengono fissate per ciascun aggiornamento; i metadati sono pubblicati dopo la validazione. Le singole sovrascritture non costituiscono una transazione atomica su più tabelle.

Tutti i controlli finali Gold risultano superati: conteggi, unicità della granularità, chiavi obbligatorie, metriche finite, quote/probabilità valide, valori di valutazione congelati ed esclusioni contro il leakage operativo. Questi controlli strutturali non risolvono la differenza semantica della sezione 14.

Metriche di rete riportate: **9,975,703 osservazioni**; media **24.7172 secondi**; mediana **-71**; P90 **644**; P95 **1,198**; P99 **3,102**. Il conteggio di ritardo grave è **1,878,569**, quota **0.18831**, secondo la definizione della sorgente registrata, non validata separatamente per >600 secondi. Le soglie di ranking sono 1,000 osservazioni per linea e 500 per fermata: 354 linee e 5,172 fermate risultano ammissibili.

---

# 34. MODELLI DI MACHINE LEARNING

La complessità dei modelli è stata aumentata solo per verificarne il valore aggiunto in validazione. Per la regressione sono stati confrontati Persistence, baseline storiche, Linear Regression, Random Forest Regressor e GBT Regressor; per la classificazione, classe maggioritaria, Logistic Regression e Random Forest.

## Regressione

- Baseline Persistence (`next delay = current delay`).
- Baseline storica.
- Linear Regression.
- Random Forest Regressor.
- Gradient Boosted Trees Regressor.
- XGBoost Regressor: estensione dipendente dall'ambiente.

## Classificazione

- Baseline della classe maggioritaria.
- Logistic Regression.
- Random Forest Classifier.
- XGBoost Classifier: estensione dipendente dall'ambiente.

I riferimenti a XGBoost non sono risultati finali completati. Le decisioni finali sono **Persistence per la regressione** e **RandomForestClassifier per la classificazione**. La maggiore complessità in regressione non ha prodotto un miglioramento sufficiente; conservare la baseline è un risultato analitico valido, non un'implementazione incompleta.

---

# 35. TUNING DEGLI IPERPARAMETRI

Il notebook 11 completa tuning e selezione su validation. La proposta iniziale di Grid Search / Randomized Search è stata tradotta in confronti espliciti tra candidati, con tracciamento MLflow e risultati intermedi persistiti; non è stato effettuato tuning guidato dal TEST.

- Si preservano finestre cronologiche train/validation e contratto delle feature congelato.
- MLflow registra iperparametri e metriche specifiche del task.
- Il TEST viene usato soltanto dopo aver fissato modello, iperparametri e soglia di probabilità.
- I confronti utilizzano versioni sorgente, finestre e metriche coerenti.

Configurazione finale del classificatore:

```text
RandomForestClassifier
numTrees = 100
maxDepth = 10
minInstancesPerNode = 5
featureSubsetStrategy = sqrt
maxBins = 32
seed = 42
```

Regola congelata: `major_delay_probability > 0.45`, in senso stretto. La soglia di probabilità è distinta dalla soglia di ritardo che definisce l'evento target; si veda la sezione 14.

---

# 36. VALUTAZIONE DEI MODELLI

## Risultati TEST finali della regressione

Persistence prevede `current_arrival_delay_seconds`, con **-71 secondi**, mediana del train, come valore sostitutivo. È l'approccio più forte in validazione; non si afferma che un modello più complesso lo abbia superato.

| Metrica | Valore |
| --- | ---: |
| MAE | 136.9818 secondi |
| RMSE | 401.8525 secondi |
| R² | 0.6940 |
| WAPE | 0.3293 |

## Risultati TEST finali della classificazione

RandomForestClassifier con configurazione congelata e confronto stretto probability >0.45:

| Metrica | Valore |
| --- | ---: |
| Accuracy | 0.9401 |
| Precision, classe positiva | 0.8450 |
| Recall, classe positiva | 0.8123 |
| F1, classe positiva | 0.8283 |
| ROC-AUC | 0.9690 |
| PR-AUC | 0.9063 |

Per lo sbilanciamento delle classi, l'accuracy è interpretata insieme a prevalenza, precision, recall e PR-AUC. La matrice di confusione fa parte della diagnostica di valutazione.

Sono risultati congelati del notebook 11, non prestazioni ricalcolate sul riaddestramento del notebook 12 o sul batch operativo. Il target implementato localmente usa >=300 secondi: modificare il testo della dashboard non li trasforma in risultati per >600 secondi.

Il MAE può essere comunicato in minuti agli interlocutori di business, mentre i valori ufficiali sopra restano in secondi. Gli esempi numerici ipotetici precedenti sono superati da questi risultati misurati.

---

# 37. VALUTAZIONE PER SEGMENTO

Le prestazioni complessive non sono sufficienti per nessuno dei due task. Il progetto di valutazione estesa considera linea, fermata, ora, giorno della settimana, gravità del ritardo, classe e avanzamento della corsa.

Risultati completi su errori e stabilità per segmento rimangono un'estensione, salvo evidenze specifiche. L'affidabilità descrittiva Gold non equivale alla valutazione degli errori del modello.

Esempio puramente illustrativo, non risultato finale misurato:

```text
MAE complessivo          2.3 min
MAE nelle ore di punta  3.1 min
MAE fuori punta         1.7 min
```

Lo sbilanciamento globale è affrontato nella valutazione completata. Un monitoraggio più ampio del bilanciamento e della stabilità per segmento rimane nel piano di estensione, con metriche appropriate al task. Serve a individuare prestazioni deboli che possono essere nascoste da metriche globali accettabili.

---

# 38. SPIEGABILITÀ DEI MODELLI

Il perimetro iniziale della spiegabilità resta una guida per le estensioni, non una dichiarazione che ogni artefatto sia stato prodotto. Persistence è direttamente interpretabile. Le analisi aggiuntive devono essere supportate dagli artefatti effettivamente disponibili:

- importanza delle feature;
- permutation importance;
- valori SHAP dove supportati;
- spiegazioni globali del comportamento del modello;
- spiegazioni locali delle singole previsioni;
- interpretazione operativa delle feature più influenti.

La documentazione delle analisi deve collegare il comportamento dei modelli alle dinamiche osservate dei ritardi, distinguendo chiaramente quanto implementato da quanto ancora previsto.

---

# 39. MLFLOW

Il tracciamento degli esperimenti è implementato in MLflow. Il suo successo è distinto dalla serializzazione degli artefatti Spark, ancora limitata dall'ambiente corrente.

L'inventario iniziale di logging comprendeva algoritmo, iperparametri, insieme/versione delle feature, finestre train e validation, metriche di regressione o classificazione, artefatti di importanza delle feature, matrice di confusione, SHAP dove disponibile, artefatto del modello e confronto tra esecuzioni. La disponibilità varia: non si garantisce un modello serializzato o un artefatto SHAP per ogni esecuzione.

Le esecuzioni identificano task, parametri, contratti delle feature, metadati di sorgente/split e metriche disponibili. Baseline e finestre confrontabili supportano la selezione. Le esecuzioni del classificatore registrano la soglia di probabilità scelta; la definizione dell'etichetta deve restare collegata alla sorgente valutata, non al testo della dashboard.

MLflow su Databricks supporta tracciamento, valutazione e registro dei modelli; la provenienza del modello può essere associata ai dati Unity Catalog. La disponibilità della funzionalità non dimostra che la registrazione sia riuscita nel progetto.

Esperimento implementato:

```text
/Shared/rome_transport_next_stop_delay
```

Esempi originali di nomi delle esecuzioni, non inventario delle esecuzioni completate:

```text
baseline_persistence
linear_v1
random_forest_v1
gbt_v1
xgboost_v1
```

---

# 40. REGISTRAZIONE DEL MODELLO

Il notebook 12 ha tentato la registrazione, che **non è stata completata**. L'attuale ambiente Databricks Serverless SparkML / Unity Catalog limita la serializzazione degli artefatti. Lo stato è `DEFERRED_SERVERLESS_LIMITATION`.

Il nome previsto per il classificatore è `rome_transport.ml.major_delay_classifier`: è una destinazione configurata, non la prova di una versione registrata in produzione. La regola Persistence non richiede un estimatore di regressione registrato.

Tracciamento MLflow, selezione finale e scoring sono completati. Il notebook 12 mantiene la pipeline completa di preprocessing e classificazione nel percorso di scoring attivo e registra i metadati del tentativo in `rome_transport.ml.model_registry_metadata`. Gli errori inattesi non sono trattati silenziosamente come limitazioni note di Serverless.

La persistenza degli artefatti e il successivo caricamento del modello in un ambiente supportato rimangono sviluppi futuri. Gli alias `candidate`/`champion` facevano parte del disegno iniziale di governance e non sono dichiarati come distribuiti.

---

# 41. BATCH SCORING

Il percorso batch implementato, con impostazione assimilabile alla produzione, è:

```text
Feature ammissibili alla previsione per l'ultima snapshot disponibile
    → Persistence + pipeline RandomForestClassifier congelata
    → rome_transport.ml.next_stop_delay_predictions
    → rome_transport.gold.predictive_operations
```

Il notebook 12 effettua il riaddestramento dopo la selezione congelata e produce previsioni senza etichette future. Gli output comprendono identificatori, contesto corrente, previsione di regressione, probabilità alla fermata successiva e flag con soglia strettamente >0.45.

La tabella di scoring conserva la provenienza dell'esecuzione e della registrazione; la proiezione operativa Gold mantiene tali informazioni nei metadati separati di aggiornamento. Le etichette future e gli errori presenti nello schema inizialmente proposto non sono inclusi in `predictive_operations`.

Ultimo batch verificato: **17,177 righe**, **2,831 previsioni con flag positivo**, quota **0.16481**. Probabilità media **0.17923**, P90 **0.86189**, P95 **0.91219**, P99 **0.95041**.

La regressione utilizza `current_arrival_delay_seconds`, con valore sostitutivo **-71 secondi** in caso di dato mancante. Il rinvio della registrazione non invalida lo scoring batch in memoria già eseguito; limita invece le dichiarazioni di distribuzione e ricaricamento basati su artefatti persistiti.

Non si tratta di un'API live o di un servizio di produzione in tempo reale. L'ultima snapshot disponibile può essere una riproduzione dello storico; un singolo batch non fornisce tendenze predittive né una nuova misura su dati di test. Fasce di rischio e definizione del target richiedono la riconciliazione della sezione 14.

---

# 42. LIVELLO ANALITICO DATABRICKS SQL

Tabelle e viste Gold costituiscono il livello analitico curato. Databricks SQL espone questi oggetti come dataset governati per Databricks AI/BI Dashboards.

```text
Livello Gold
      ↓
Databricks SQL
      ↓
Databricks AI/BI Dashboards
```

La dashboard completata utilizza esclusivamente gli otto oggetti Gold. L'SQL esportato non contiene collegamenti diretti a Bronze o Silver. La validazione strutturale è superata; la differenza tra le definizioni delle metriche è documentata separatamente nella sezione 14.

La dashboard **Rome Public Transport Reliability**, con titolo italiano **Affidabilità del Trasporto Pubblico di Roma**, è pubblicata in Databricks e richiede autenticazione. Non è disponibile un accesso pubblico anonimo: è un vincolo di distribuzione del portfolio, non un malfunzionamento di Databricks.

Il repository include l'[export JSON](../dashboard/databricks/dashboard/Affidabilita_Trasporto_Pubblico_Roma.lvdash.json) e il [PDF completo della dashboard](../dashboard/databricks/pdf/Affidabilita_Trasporto_Pubblico_Roma.pdf). Il PDF contiene cinque pagine nell'ordine: Panoramica Esecutiva, Affidabilità della Rete, Propagazione dei Ritardi, Monitoraggio Predittivo, Prestazioni del Modello. È un artefatto statico consultabile senza autenticazione Databricks, non un servizio interattivo pubblico. Una presentazione pubblica Power BI è pianificata separatamente. Pipeline tecnica, logica analitica e Machine Learning rimangono basati su Databricks.

---

# 43. DASHBOARD, PAGINA 1 — PANORAMICA ESECUTIVA

**Pagina implementata:** Panoramica Esecutiva. L'interfaccia italiana è intenzionale per Roma e gli interlocutori della mobilità locale. La pagina presenta KPI storici di rete e sintesi dell'ultimo scoring.

L'inventario progettuale seguente conserva le motivazioni originarie, senza implicare che ogni grafico o approfondimento sia implementato. L'export della dashboard è il riferimento per il layout effettivo.

**Finalità:** offrire una lettura immediata dell'affidabilità della rete.

KPI inizialmente considerati: ritardo medio e mediano, percentuale di regolarità, percentuale di ritardi gravi, linee attive, corse osservate, MAE di regressione e F1/rischio di classificazione dove pertinenti.

Visualizzazioni ipotizzate: andamento temporale dei ritardi, classifica di affidabilità delle linee, affidabilità oraria e confronto per giorno della settimana.

---

# 44. DASHBOARD, PAGINA 2 — AFFIDABILITÀ DELLA RETE

**Pagina implementata:** Affidabilità della Rete. Presenta confronti per linea, fermata e tempo, applicando i requisiti minimi di numerosità per il ranking.

**Finalità:** individuare dove si concentrano i problemi. L'inventario iniziale prevedeva una mappa di calore linea × ora, classifiche delle fermate, distribuzione dei ritardi, P90 e andamento delle linee. I filtri proposti riguardavano linea, data, giorno della settimana, ora e direzione.

Questi elementi conservano il disegno originario; non attestano che ogni visualizzazione o filtro sia presente. Il layout effettivo è quello dell'export. L'interfaccia italiana risponde al contesto locale del progetto.

---

# 45. DASHBOARD, PAGINA 3 — PROPAGAZIONE DEI RITARDI

**Pagina implementata:** Propagazione dei Ritardi. Presenta stati di ritardo corrente/precedente e associazioni con gli esiti realizzati alla fermata successiva.

**Finalità:** comprendere come il ritardo evolve lungo una corsa. Le visualizzazioni inizialmente ipotizzate erano sequenza delle fermate rispetto al ritardo, ritardo cumulato, incremento medio per segmento e confronto tra linee.

L'inventario è una motivazione progettuale, non una dichiarazione di implementazione di ogni grafico. La pagina completata comunica associazioni di propagazione, non effetti causali; il layout effettivo è documentato dall'export.

---

# 46. DASHBOARD, PAGINA 4 — MONITORAGGIO PREDITTIVO

**Pagina implementata:** Monitoraggio Predittivo. Presenta previsioni batch del ritardo, probabilità alla fermata successiva e informazioni utili a stabilire priorità operative.

**Finalità:** tradurre gli output ML in informazioni leggibili dagli interlocutori operativi. L'inventario originario considerava linea, veicolo, fermata corrente, ritardo corrente, ritardo successivo previsto, probabilità e flag di ritardo grave, media storica e livello di rischio. Le categorie iniziali erano Normale, Elevato, Alto e Critico.

L'inventario resta un riferimento progettuale; non afferma che ogni colonna o visualizzazione sia presente. La presentazione finale dichiarata usa **Basso p<=0.30, Medio 0.30<p<=0.50, Alto 0.50<p<=0.70, Critico p>0.70**.

Le fasce visuali sono distinte dal flag congelato **p>0.45** e differiscono dalle fasce attualmente implementate in Gold. La riconciliazione resta da completare; non si attribuisce alle soglie di presentazione un'ottimalità statistica. L'export è il riferimento del layout effettivo.

---

# 47. DASHBOARD, PAGINA 5 — PRESTAZIONI DEL MODELLO

**Pagina implementata:** Prestazioni del Modello. Mostra le metriche congelate di regressione e classificazione, attraverso i dati Gold, rendendo trasparenti le scelte finali.

L'inventario progettuale originario comprendeva:

### Regressione

Confronto osservato/previsto, MAE, RMSE, distribuzione degli errori, MAE nel tempo e per linea/ora.

### Classificazione

Precision, recall, F1, ROC-AUC, PR-AUC, matrice di confusione, distribuzione delle probabilità e prestazioni per linea/ora.

### Confronti trasversali

Confronto tra modelli e tra baseline e modelli ML.

Le metriche finali sono riferimenti fissi. L'elenco conserva il perimetro progettuale, non prova che tutti i grafici siano presenti o che siano state ricalcolate prestazioni sul riaddestramento. Il layout effettivo resta quello dell'export.

---

# E — ESECUZIONE

# 48. PERCORSO DI IMPLEMENTAZIONE

Le fasi mantengono la logica del piano iniziale. Per ciascuna, l'esito distingue quanto realizzato dalle attività ancora previste.

## Fase 1 — Ambiente

**Esito:** implementata con il notebook 01 e la configurazione del repository.

Il piano prevedeva workspace Databricks, struttura Unity Catalog, organizzazione del repository e configurazione dell'ambiente.

## Fase 2 — Ingestion GTFS Static

**Esito:** implementata nei notebook 02–03.

Il piano prevedeva acquisizione e validazione di `routes`, `trips`, `stops`, `stop_times`, `calendar` e `calendar_dates`, per ottenere un modello di riferimento stabile della rete.

## Fase 3 — Ingestion realtime

**Esito:** implementata nei notebook 04–05.

Acquisizione di Trip Updates, Vehicle Positions e Service Alerts, con snapshot Bronze associate al timestamp.

## Fase 4 — Accumulo storico

**Esito:** archivio costruito; l'analisi finale copre 964 snapshot e otto date di servizio.

Il piano richiedeva una storia sufficiente per l'analisi, con test dell'ingestion, monitoraggio dei feed, profilazione, miglioramento dello schema e controlli di qualità.

## Fase 5 — Modello Silver

**Esito:** implementata nei notebook 03, 06 e 07.

Normalizzazione delle entità e associazione tra orari statici, Trip Updates, posizioni e avvisi, per ottenere uno storico operativo pulito.

## Fase 6 — Analisi esplorativa

**Esito:** profilazione e diagnostica integrate nel percorso; non si dichiara un notebook EDA separato.

Il piano prevedeva analisi di distribuzioni, linee, fermate, comportamento temporale, completezza e propagazione. La profilazione ha orientato la formulazione delle etichette future e il filtro di qualità implementati.

## Fase 7 — Feature Engineering

**Esito:** implementata nel notebook 08.

L'obiettivo era una tabella riutilizzabile, concretizzata in `features.next_stop_delay_features`.

## Fase 8 — Baseline

**Esito:** implementate e valutate; Persistence mantenuta come regressione finale.

Le regole semplici forniscono il riferimento rispetto al quale valutare l'utilità dei modelli più complessi.

## Fase 9 — Training ML

**Esito:** training e valutazione completati nei notebook 10–11; la spiegabilità estesa rimane uno sviluppo futuro.

Il piano prevedeva confronto di modelli lineari, ad alberi e boosting per la regressione; definizione del target, analisi dello sbilanciamento, baseline e training per la classificazione; tuning, spiegabilità e valutazione congiunta su validation. Il tracciamento avviene con MLflow.

## Fase 10 — Selezione dei modelli

**Esito:** completata su validation, con decisioni congelate prima del TEST.

I criteri progettuali includono metriche per entrambi i task, stabilità per segmento, interpretabilità, costo computazionale e miglioramento sulle baseline, non la sola metrica migliore. Una volta conclusi selezione e tuning, la valutazione finale viene eseguita sul TEST rimasto escluso dalle decisioni. I suoi risultati non devono essere usati per selezionare o ritoccare i modelli.

## Fase 11 — Registro dei modelli

**Esito:** tentativo effettuato ma rinviato per la serializzazione degli artefatti in Serverless; nessun modello registrato in produzione.

Il piano prevedeva registrazione e documentazione di feature, periodo di training, metriche, limiti e versione del modello.

## Fase 12 — Inferenza batch

**Esito:** completata nel notebook 12 sull'ultima snapshot disponibile.

Produce ritardo previsto, probabilità di ritardo grave e relativo flag, con impostazione assimilabile alla produzione.

## Fase 13 — Analisi Gold

**Esito:** completata nel notebook 13 con otto tabelle analitiche per snapshot, non con i nomi dimensionali iniziali.

L'obiettivo è fornire tabelle pronte per BI; l'inventario implementato è nella sezione 33.

## Fase 14 — Databricks AI/BI Dashboards

**Esito:** dashboard completata e pubblicata all'interno di Databricks; è richiesta autenticazione.

Le cinque pagine usano dataset Databricks SQL provenienti esclusivamente da oggetti Gold curati.

## Fase 15 — Documentazione

**Esito:** README e PACE aggiornati, export JSON e PDF completo disponibili. Dizionario dei dati e scheda del modello autonomi, oltre alla presentazione pubblica Power BI, restano futuri.

Il piano comprendeva README.md, diagramma architetturale, dizionario dei dati, metodologia ML, scheda del modello, documentazione della dashboard, limiti e miglioramenti futuri.

---

# 49. STRUTTURA DEL REPOSITORY

```text
README.md
.gitignore
dashboard/
  databricks/
    dashboard/
      Affidabilita_Trasporto_Pubblico_Roma.lvdash.json
    pdf/
      Affidabilita_Trasporto_Pubblico_Roma.pdf
  powerbi/
    README.md
    report/
    screenshots/
data/
  reference/
    gtfs_static/  [file sorgente locali ignorati da Git]
docs/
  PACE.md
notebooks/
  01_environment_setup.sql
  02_gtfs_static_bronze_ingestion.py
  03_gtfs_static_silver.py
  04_gtfs_realtime_ingestion.py
  05_gtfs_realtime_pipeline.py
  06_gtfs_realtime_silver.py
  07_gtfs_realtime_enrichment.py
  08_feature_engineering.py
  09_target_labeling.py
  10_model_training.py
  11_model_tuning_and_selection.py
  12_model_registration_and_scoring.py
  13_gold_analytics.py
  99_realtime_pipeline_health_check.sql
```

Git non versiona le directory vuote. Le impostazioni locali del workspace dell'editor sono ignorate e non compaiono nella struttura destinata alla pubblicazione.


La struttura rispecchia i notebook ordinati 01–13 e il notebook 99. Le directory vuote inutilizzate sono state rimosse. I file GTFS locali di grandi dimensioni restano su disco ma sono esclusi da `.gitignore`: non costituiscono dataset pubblicati. I binari PBIX/PBIT sono ignorati; PBIP/PBIR e definizioni testuali dei modelli semantici possono essere versionati.

---

# 50. QUADRO DEI CONTROLLI DI QUALITÀ

I controlli implementati coprono unicità della granularità, chiavi obbligatorie, coerenza dei timestamp, causalità delle etichette, disponibilità retrospettiva delle posizioni, filtro di qualità degli esiti ed esclusioni contro il leakage nello scoring.

Gold verifica output non vuoti, metriche finite, quote/probabilità in [0,1], corrispondenza esatta con le metriche congelate, numerosità minima per il ranking e riconciliazione prima e dopo la scrittura.

Rimangono rilevanti le diagnostiche specifiche dei livelli: linee/fermate mancanti, sequenze non valide, snapshot ripetute e osservazioni obsolete. Il superamento dei controlli strutturali non dimostra che la semantica >600 secondi della dashboard corrisponda al target computazionale >=300 secondi.

## Decisione sulla qualità delle etichette realizzate


Il dataset esteso analizzato il 2 ottobre conteneva **10,007,020 righe etichettate prima del filtro finale di qualità**. I conteggi diagnostici erano cumulativi:

| Ritardo realizzato in valore assoluto | Righe |
| --- | ---: |
| > 1 ora | 104,719 |
| > 2 ore | 39,008 |
| > 4 ore | 33,850 |
| > 12 ore | 31,317 |

Le **31,317 etichette oltre ±12 ore rappresentano circa lo 0.31%** delle 10,007,020 etichette realizzate. Di queste, **31,249 ricadevano nelle ore del feed comprese tra le 21:00 e le 03:59**: circa il **99.8% del solo gruppo >12 ore**. Appena il **36.3%** di tale gruppo superava già ±12 ore nel target GTFS-RT provvisorio.

Questa concentrazione supporta l'interpretazione di artefatti legati al passaggio della mezzanotte, alla gestione del giorno di servizio GTFS e alla temporizzazione dei feed, anziché di ritardi operativi plausibili alla fermata successiva. La regola di inclusione nel training è:

```python
MAX_PLAUSIBLE_ABS_REALIZED_DELAY_SECONDS = 43200
```

```sql
ABS(target_realized_next_stop_delay_seconds) <= 43200
```

Si tratta di una **regola di qualità fondata su evidenze**, non di rimozione arbitraria degli outlier, winsorization, clipping o filtraggio guidato dal modello. I ritardi oltre 1, 2 o 4 ore rimangono ammissibili entro ±12 ore. Le etichette escluse non vengono alterate o corrette artificialmente; i target provvisori restano invariati.

Il filtro preserva ritardi severi ma plausibili ed evita che artefatti di circa 24–29 ore distorcano MAE, RMSE, media del target e training della regressione. Soglia ed evidenze rendono la decisione riproducibile e verificabile: non implicano che tutti i dati notturni siano invalidi, né che ±12 ore sia una soglia statisticamente ottimale o universalmente valida per GTFS. Il filtro è applicato: **10,007,020 - 31,317 = 9,975,703 osservazioni etichettate finali**.


Le diagnostiche delle etichette future hanno riportato zero etichette nella stessa snapshot, zero etichette riferite al passato e zero righe duplicate aggiuntive. I valori estremi esclusi non vengono corretti o sottoposti a clipping; i target provvisori restano invariati. Sono regole riproducibili della pipeline, non filtri guidati dal modello.

---

# 51. RISCHI DEL PROGETTO

## Disponibilità limitata dello storico realtime

Mitigazione: costruire un archivio proprio di snapshot, distinguendo periodo osservato e generalizzabilità.

## Osservazioni realtime mancanti

Mitigazione: misurare la completezza del feed e non presumere un tracciamento continuo.

## Variazioni GTFS

Gli orari statici possono cambiare. La strategia di mitigazione è versionare l'ingestion GTFS Static, anziché trattare il riferimento come immutabile.

## Ripetizione dei record

Mitigazione: timestamp della sorgente e chiavi naturali per la deduplica.

## Data leakage

Mitigazione: feature coerenti con la disponibilità temporale e validazione cronologica.

## Miglioramento limitato dei modelli complessi

Questo rischio si è tradotto in un risultato utile: Persistence è rimasta la soluzione di regressione più forte in validazione. Non è stato preferito un estimatore più complesso senza evidenze sufficienti di miglioramento.

## Limiti residui dell'ambiente e dell'interpretazione

La registrazione è rinviata per la serializzazione Serverless. La dashboard interattiva richiede autenticazione; il PDF e l'export JSON ne consentono la presentazione nel portfolio senza attribuirle accesso pubblico live. Otto date di servizio limitano la generalizzazione; gli esiti futuri GTFS-Realtime sono proxy operativi e la causalità dei timestamp non prova l'esatta disponibilità in ingestion. Le definizioni di ritardo e rischio ML/dashboard richiedono riconciliazione, come documentato nella sezione 14.

---

# 52. CRITERI DI ACCETTAZIONE ML

Il requisito iniziale secondo cui un modello complesso doveva superare Persistence è stato rivisto: si conserva l'approccio più forte in validazione, anche quando è una baseline.

1. Mantenere Persistence salvo un miglioramento sufficiente su validation da parte di un approccio più complesso.
2. Confrontare il classificatore con baseline significative e metriche adeguate allo sbilanciamento; la scelta finale è RandomForestClassifier.
3. Valutare decisioni congelate su osservazioni future in ordine cronologico, escluse dalla selezione.
4. Escludere dai predittori posizioni future, aggregati con snapshot successive, target provvisori e provenienza delle etichette.
5. Stimare il preprocessing sul train durante la valutazione ed eliminare le etichette che oltrepassano i confini temporali dello split.
6. Tracciare iperparametri, contratti delle feature ed esecuzioni di valutazione con MLflow.
7. Congelare le decisioni di probabilità prima del TEST: la regola finale è strettamente p>0.45.
8. Preservare i risultati TEST finali durante il riaddestramento per batch scoring.
9. Documentare il rinvio della registrazione separatamente dalla validità dello scoring.
10. Mantenere tra le attività residue la stabilità estesa per segmento, la spiegabilità completa e la riconciliazione semantica quando le evidenze non sono complete.

Questi criteri non implicano che tutte le estensioni desiderate o tutti i requisiti di distribuzione in produzione siano già soddisfatti.

---

# 53. CRITERI DI ACCETTAZIONE DELLA DASHBOARD DATABRICKS AI/BI

Le cinque pagine implementate affrontano le domande di business originarie tramite dataset Gold curati, senza richiedere l'apertura dei notebook tecnici:

1. Quanto è affidabile la rete?
2. Quali linee hanno le prestazioni peggiori?
3. Quali fermate sono associate ad accumuli di ritardo?
4. Quando i ritardi sono più frequenti?
5. Come si propaga il ritardo lungo una corsa?
6. Quali ritardi prevede il modello nella snapshot corrente?
7. Qual è la probabilità prevista di ritardo grave?
8. Quali sono le prestazioni dei modelli di regressione e classificazione?
9. In quali contesti i modelli funzionano peggio?

L'analisi dettagliata degli errori per segmento rimane un'estensione, non un approfondimento già dichiarato come consegnato. La riconciliazione semantica della sezione 14 resta necessaria.

---

# 54. DELIVERABLE DEL PORTFOLIO

## Componenti tecniche e documentali completate

- Notebook Databricks ordinati 01–13 e notebook 99 di controllo.
- Elaborazioni Bronze/Silver, dataset di feature/etichette, esperimenti MLflow e valutazione finale.
- Scoring con impostazione assimilabile alla produzione e otto tabelle Gold validate.
- Dashboard AI/BI su cinque pagine, alimentata da Databricks SQL e pubblicata con autenticazione.
- Export `.lvdash.json` e PDF completo di cinque pagine, README e metodologia PACE dettagliata.

## Componenti rinviate o future

- Registrazione riuscita degli artefatti in Unity Catalog e verifica di distribuzione/ricaricamento.
- Riconciliazione delle definizioni di ritardo/rischio tra presentazione della dashboard e ML/Gold.
- Presentazione Power BI pubblica separata, descritta in `dashboard/powerbi/README.md`.
- Dizionario dei dati e scheda del modello autonomi, spiegabilità più ampia e monitoraggio su un orizzonte maggiore dove non ancora documentati.

Non si dichiara un modello registrato, una dashboard live pubblica anonima o un report Power BI completato. L'accesso Databricks è un vincolo di distribuzione del portfolio, non un errore tecnico.

---

# 55. RACCONTO DEL PROGETTO

Il progetto implementato può essere descritto così:

> Ho progettato una piattaforma end-to-end di analisi e Machine Learning basata sui dati ufficiali del trasporto pubblico di Roma. Ho costruito pipeline di ingestion GTFS Static e realtime, creato livelli Bronze/Silver/Gold in Delta Lake, sviluppato feature temporali e operative, addestrato e tracciato modelli di regressione e classificazione con MLflow. Ho mantenuto Persistence per la regressione e selezionato Random Forest per la classificazione, documentato il rinvio della registrazione in Unity Catalog, generato previsioni batch con impostazione assimilabile alla produzione e reso disponibili analisi operative e prestazioni dei modelli tramite Databricks SQL e AI/BI Dashboards.

La scelta end-to-end di Databricks mantiene il ciclo analitico in una piattaforma coerente con l'obiettivo del progetto.

```text
Ingestion dei dati
      ↓
Lakehouse
      ↓
Data Engineering
      ↓
Machine Learning
      ↓
MLOps
      ↓
Analisi SQL
      ↓
Dashboard AI/BI
```

Il lavoro dimostra competenze che vanno oltre il training: Data Engineering, qualità dei dati, Spark, SQL, modellazione analitica e dimensionale, Machine Learning, MLOps e Business Intelligence. La modellazione Gold effettiva è quella delle tabelle analitiche documentate, non una dichiarazione di implementazione di ogni dimensione inizialmente proposta.

---

# 56. PRINCIPIO GUIDA

La selezione finale segue un principio:

> **Introdurre complessità solo quando i dati dimostrano che aggiunge valore.**

```text
Comprendere i dati
      ↓
Costruire una pipeline affidabile
      ↓
Definire una baseline
      ↓
Misurare
      ↓
Introdurre modelli ML
      ↓
Misurare nuovamente
      ↓
Rendere visibile il valore operativo
```

Il modello di Machine Learning è una componente del progetto, non il progetto nella sua interezza.

---

# 57. DEFINIZIONE FINALE DEL PROGETTO

**Progetto:** Rome Public Transport Reliability & Delay Prediction

**Dominio:** mobilità urbana e operazioni del trasporto pubblico

**Architettura dei dati:** Medallion Lakehouse

**Piattaforma principale:** Databricks

**Elaborazione:** Spark / PySpark / SQL

**Archiviazione:** Delta Lake

**Governance:** Unity Catalog

**Tracciamento ML:** MLflow

**Problema ML:** regressione del ritardo a breve orizzonte come task principale; classificazione binaria del ritardo grave come task secondario

**Validazione:** cronologica

**Inferenza:** batch scoring con impostazione assimilabile alla produzione

**Livello analitico:** otto tabelle Gold per snapshot analitiche

**Accesso alle analisi:** Databricks SQL + Gold

**Business Intelligence:** Databricks AI/BI Dashboards

**Risultato principale:** un sistema riproducibile di analisi e Machine Learning che trasforma i feed realtime del trasporto pubblico di Roma in conoscenza operativa storica e previsioni del ritardo a breve termine.

## Stato finale ed estensioni

L'implementazione Databricks è sostanzialmente completa fino a scoring, analisi Gold e dashboard AI/BI autenticata. La registrazione in produzione resta rinviata. La riconciliazione esplicita delle definizioni di ritardo/rischio è il prossimo miglioramento metodologico di allineamento tra codice e documentazione: questa revisione modifica soltanto testi. Il PDF completo è già disponibile nel repository; la presentazione pubblica Power BI rimane un'estensione separata del portfolio.

---

