# Runtime del modello: target operativo esterno e riferimento Spark storico

**ML operativo: target backend Python esterno.** FastAPI/Python/scikit-learn gestirà training, valutazione, artifact completi del modello addestrato, caricamento e inference nel futuro repository `rome-mobility-intelligence`. Questa PR di handoff non implementa backend o modello. [ADR-001](ARCHITECTURE_DECISIONS.md), [contratto dati/accesso](OPERATIONAL_ML_HANDOFF.md), [roadmap](ROADMAP.md).

**Spark ML: ML storico / sperimentale - implementazione di ricerca / riferimento.** Selezione congelata del notebook 11, notebook storico 12 e implementazione/test PR4 restano preservati. Le sezioni tecniche seguenti descrivono solo quel disegno storico/di riferimento; non sono il piano di rollout operativo. Fit operativo PR4, persistenza dell'artifact, ricaricamento nativo e scoring non sono mai stati eseguiti. Nessuna modifica a storico delle prediction, Gold, ingestion o Job.

## Decisione runtime verificata, 25 ottobre 2026

Le impostazioni esplicite `mlflow.set_tracking_uri("databricks")` e `mlflow.set_registry_uri("databricks-uc")` hanno abilitato selezione dell'esperimento, creazione della run e logging dei parametri. L'accesso isolato allo storage su Volume è passato. Il nuovo smoke test di serializzazione è poi fallito alla costruzione di `VectorAssembler`, con `Py4JSecurityException` sulla whitelist dei costruttori Spark Connect, prima del fit minimo o della creazione dell'artifact. Non è seguito alcun fit operativo.

Una richiesta autorizzata di compute classic Dedicated 16.4 LTS ML CPU è stata rifiutata perché l'organization non ha worker environment associati. Nessun cluster è stato creato o lasciato attivo. Il target quindi non dipende più da persistenza Spark, compute classic a pagamento o registrazione UC. Le future scelte di algoritmo/preprocessing/soglia sklearn richiedono una nuova valutazione; le impostazioni Spark storiche non sono una specifica di conversione.

`historical_model_threshold = 0.45` (strettamente >); **futura soglia del modello operativo: da validare nella valutazione del nuovo modello**. Le metriche TEST storiche sotto riportate appartengono solo a Spark/Persistence. Il training v12 è validato; v8 resta il riferimento storico e non è più leggibile per retention/VACUUM. Nessun fallback automatico di versione o rietichettatura è autorizzato.

## Audit e architettura

La run storica `a9b568f478344d9dba90436358f14092` ha stato `DEFERRED_SERVERLESS_LIMITATION`, nessun URI di artifact e dimensione codificata 475. Un controllo dei metadati in sola lettura ha confermato l'errore MLflow registrato: per save/log/load SparkML su compute shared/serverless era richiesto un percorso di Volume UC esistente. Il vecchio notebook tentava logging dell'artifact e registrazione insieme, rinviava l'errore noto e proseguiva con la pipeline addestrata in memoria. Non forniva quindi un artifact ricaricabile indipendentemente. Cambiare percorso non ha dimostrato la persistenza nativa: lo smoke test successivo si è fermato alla costruzione Spark ML. Questo percorso è ora solo di riferimento.

Il diagramma di inference conservato è:

```text
input grezzi ordinati: 42 numerici + 5 categorici
  -> proiezione SQL appresa / ImputerModel
  -> StringIndexerModel / OneHotEncoderModel appresi
  -> VectorAssembler / StandardScalerModel appreso
  -> RandomForestClassificationModel addestrato (usa feature non scalate)
  -> vettore di probabilità nativo, ordine diretto delle classi verificato [0, 1]
  -> P(classe 1) -> probabilità strettamente > 0.45
```

Lo scaler è mantenuto perché era presente nel preprocessing storico, anche se RF usa `features`, non `scaled_features`. PR4 non addestra né persiste alcun estimatore di regressione.

## File e responsabilità

- `12_common_persisted_classifier`: definizioni per preprocessing, validazione dei metadati dell'artifact, caricamento nativo e scoring; nessuna azione runtime all'importazione. Caricarlo dopo `08_common_feature_preparation` con celle `%run` relative separate.
- `12a_fit_persist_final_classifier`: refit di deployment esplicito sulla popolazione storica ammissibile, persistenza obbligatoria dell'artifact completo, ricaricamento nativo/parità e nuove diagnostiche in-sample. La registrazione UC opzionale segue la validazione riuscita dell'artifact.
- `12b_score_current_input`: fissa `features.next_stop_delay_scoring`, carica un artifact esistente e chiama solo transform. Nessuna lettura della sorgente etichettata, fit, logging MLflow, registrazione o scrittura della tabella di output.

I notebook originali 09–12 e la logica PR3 di preparazione staging sono invariati. `13_gold_analytics` non viene rinumerato.

## Contratto immutabile di modello/feature

Le 47 feature grezze restano nell'ordine congelato, con 42 input numerici e 5 categorici: route_id, route_type, direction_id, agency_id e current_status. Hash:

`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`

L'evento è `next_stop_delay_ge_300s`; l'etichetta positiva è il ritardo proxy GTFS-RT realizzato alla fermata successiva **>=300 secondi**. Il flag di business usa **strettamente p>0.45**. Esattamente 0.45 resta negativo; la prediction predefinita RF non viene usata per questo flag.

Parametri RandomForestClassifier congelati: numTrees=100, maxDepth=10, minInstancesPerNode=5, featureSubsetStrategy=sqrt, maxBins=32, seed=42. L'ordine nativo delle classi è stabilito richiedendo esattamente etichette numeriche dirette [0,1], verificando una RF binaria ed escludendo qualsiasi stage di indicizzazione delle etichette. I metadati registrano quest'ordine e lo scorer ne deriva l'indice positivo. Metadati di classe sconosciuti o riordinati vengono rifiutati.

Preprocessing invariato: cast numerici grezzi DOUBLE, imputazione mediana con relativeError=0.001, cast categorici STRING, StringIndexer alfabetico con handleInvalid=keep, OneHotEncoder con handleInvalid=keep/dropLast=False, assembler handleInvalid=error, scaler withStd=True/withMean=False. I NULL delle feature grezze passano al preprocessing appreso; durante inference non viene introdotta nuova imputazione.

Vocabolari degli indexer e dimensioni delle categorie dell'encoder persistiti gestiscono nuovi valori route/agency/status e categorie NULL tramite i bucket keep esistenti. Il loader verifica queste impostazioni e la sequenza completa degli stage appresi. La dimensione assemblata deve concordare con le dimensioni delle categorie apprese e RF numFeatures. Il valore storico 475 è un riferimento, non un requisito rigido: un nuovo fit può osservare livelli categorici diversi. Qualsiasi variazione viene marcata con un tag e va revisionata durante il rollout. Etichette/statistiche non vengono riapprese per adattarsi a un nuovo batch di scoring.

## Formato dell'artifact, metadati e caricamento

Usare il [flavor Spark nativo di MLflow](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.spark.html) per registrare la Spark PipelineModel completa. Il solo classificatore non è sufficiente. La chiamata di logging incorpora i metadati `rome_transport_classifier` nel file MLmodel dell'artifact; non introduce pickle, serializzazione manuale del preprocessing o pyfunc personalizzata. MLflow può includere i flavor ausiliari standard, ma questa implementazione carica esclusivamente con `mlflow.spark.load_model`.

I metadati includono versione dello schema dell'artifact, model_run_id, contratto esistente esatto di feature/preprocessing e feature grezze ordinate, algoritmo/iperparametri, evento/soglia del target, regola decisionale stretta, ordine delle classi, sorgente/versione di training fissate, cutoff di deployment, timestamp di training, SHA Git esatto del codice, dimensione codificata effettiva e dimensioni delle categorie apprese. Valori di imputazione e mappature categoriche appresi risiedono negli stage nativi appresi, anziché in vocabolari JSON duplicati.

`MODEL_URI` è obbligatorio e non ha un valore fisso specifico del workspace. Usare l'URI restituito dal logging riuscito, per esempio `runs:/<new_run_id>/classifier_pipeline`, un ID immutabile di modello registrato tramite logging o una versione numerata esatta del modello. Alias/stage mutabili del registro vengono rifiutati per evitare di risolvere artifact diversi fra ispezione dei metadati e caricamento nativo. Lo scoring non dipende mai dalla registrazione UC. ID della run MLflow di origine, metadati, stage appresi, parametri e dimensioni vengono validati prima di inference.

`MODEL_DFS_TMPDIR` può puntare a un percorso `/Volumes/...` esistente e autorizzato se richiesto da MLflow Spark shared/serverless. Il valore viene passato a logging e caricamento nativo, compreso il ciclo immediato di persistenza/ricaricamento. Nulla crea un volume, cambia permessi o sostituisce implicitamente compute/storage. Runtime compatibile e percorso scrivibile vanno validati separatamente. Gli errori di logging/caricamento propagano; non esiste fallback allo scoring in memoria né dichiarazione di successo senza artifact ricaricato. La registrazione nel registro è disabilitata per default e best effort dopo la validazione obbligatoria dell'artifact; il suo fallimento non può nascondere errori dell'artifact.

## Popolazione del fit e metriche

Il percorso di fit preserva la popolazione di refit di deployment del notebook storico 12: versione Delta etichettata fissata, osservazioni ed epoche sorgente delle etichette future strettamente precedenti al cutoff di deployment acquisito, etichette dirette 0/1 valide derivate da >=300 secondi, orizzonte 1–1800 secondi e regola esistente di qualità delle etichette ±43,200 secondi. Sono richieste entrambe le classi e chiavi non duplicate. Tutte le colonne numeriche congelate devono avere valori utilizzabili nel training; nessuna viene rimossa implicitamente. Nessuno split casuale, nuova ricerca di iperparametri, tuning della soglia o selezione delle feature.

La versione sorgente di training predefinita è l'originale v8. Se i file conservati non sono disponibili, fermarsi: diversamente dal vecchio 12, non è consentita la sostituzione automatica con l'ultima versione. Un successivo rollout può autorizzare esplicitamente un'altra versione fissata solo dopo aver verificato l'idoneità del contratto popolazione/etichette. Le feature non vengono mai inferite dalla disponibilità di colonne opzionali in un dataset più recente.

È un refit di deployment su tutte le etichette storiche ammissibili, coerente con il vecchio 12, non un nuovo esercizio di selezione 70/15/15. Split cronologico/purging ed evidenze TEST storiche di 10/11 restano intatti. La nuova run calcola dopo il ricaricamento le proprie metriche `new_fit_in_sample_*`: accuracy, precision, recall, F1, ROC-AUC e PR-AUC. Sono diagnostiche sulla popolazione di training e non dimostrano generalizzazione indipendente. Non si dichiarano nuove prestazioni su holdout; la valutazione su periodi effettivamente futuri resta separata.

Restano le metriche TEST storiche di riferimento:

| Task | Metrica storica | Valore |
| --- | --- | ---: |
| Regressione Persistence | MAE secondi | 136.9818 |
| Regressione Persistence | RMSE secondi | 401.8525 |
| Regressione Persistence | R² | 0.6940 |
| Regressione Persistence | WAPE | 0.3293 |
| Classificatore | Accuracy | 0.9401 |
| Classificatore | Precision | 0.8450 |
| Classificatore | Recall | 0.8123 |
| Classificatore | F1 | 0.8283 |
| Classificatore | ROC-AUC | 0.9690 |
| Classificatore | PR-AUC | 0.9063 |

Questi valori non vengono registrati come metriche raggiunte dal nuovo fit. Impostazioni e seed uguali non provano uguaglianza di parametri appresi, vocabolario categorico, dimensione del vettore o previsioni; serve un confronto runtime.

## Input/output di scoring e gate

Il percorso di scoring legge una singola versione staging fissata, verifica ordine/tipi/hash esatti delle feature grezze, una sola lineage di preparazione e una sola epoca del feed, e rifiuta colonne di etichette/esiti/prediction. Solo i campi documentati di identità della fermata successiva possono usare il prefisso target_. I campi di identità/servizio/veicolo passano all'output senza essere assemblati nel vettore del modello. Il DataFrame di output contiene chiave dell'osservazione, ID della fermata successiva, identità veicolo/servizio/orario di partenza, P(classe 1), flag stretto, model_run_id, feature_contract_hash, decision_threshold, model_algorithm e scored_at, conservando anche la lineage di preparazione staging esistente. Conteggio righe e multinsiemi delle identità devono essere preservati, le probabilità finite/nell'intervallo/con somma uno, la dimensione del vettore invariata rispetto all'artifact caricato.

Tutti i gate sono OFF per default: ALLOW_MODEL_FIT, ALLOW_MODEL_PERSIST, ALLOW_UC_REGISTRATION, ALLOW_MODEL_INFERENCE e ALLOW_SCORING_WRITE. Il fit richiede gate di fit e persistenza attivi, sorgente fissata e commit esatto del codice prima di iniziare. Run MLflow già attive vengono rifiutate, evitando scritture accidentali nella run storica. Lo scoring non ha un writer nemmeno cambiando il flag di scrittura. Il precedente piano Spark PR5 è sostituito: lo storico permanente delle prediction appartiene alla fase F della roadmap dopo la validazione del modello esterno. La tabella next_stop_delay_predictions esistente non viene sovrascritta.

**L'input scoring è ora aggiornato al batch validato del 26 ottobre.** PR3 aveva validato la snapshot del 2 ottobre su Silver v8; il nuovo stato, le versioni e la missingness sono nel [report downstream](DOWNSTREAM_RUNTIME_ROLLOUT.md). Questo aggiorna gli input, non il modello: nessun artifact PR4 o nuovo scoring è stato prodotto. Prediction e metriche Spark rimangono storiche; l'anzianità del feed resta esplicita e non si dichiarano qualità predittiva o inference live.

## Validazione locale dell'implementazione di riferimento preservata

I test offline usano stage/pipeline appresi simulati ed eseguono le definizioni effettive di caricamento/scoring. Coprono contratto/hash/parametri, ordine diretto delle classi, soglia stretta, identità preservata, categorie nuove/NULL, propagazione dello storage al caricamento, persistenza/ricaricamento obbligatori, gate predefiniti e assenza di fit/lettura del dataset etichettato/scritture di tabelle durante inference. Sono test di contratto, non prova che persistenza Spark o vettori categorici funzionino sul runtime target.

Il precedente piano di rollout Spark richiedeva uno smoke test di serializzazione prima
di qualsiasi fit completo, poi persistenza dell'artifact completo, ricaricamento nativo/parità,
test su categorie sconosciute/NULL e inference indipendente, con registrazione UC opzionale.
Il tentativo effettivo si è fermato al primo requisito. Implementazione originale e
documentazione precedente restano nella storia Git; questo piano sostituito non è
un'istruzione a riprendere il deployment Spark.

Nessun artifact operativo PR4 è stato prodotto o caricato. Codifiche categoriche Spark e parità del ciclo persistenza/ricaricamento restano non verificate in questo workspace; non si deduce successo dai test offline. La futura accettazione operativa appartiene al backend esterno e alla [nuova roadmap](ROADMAP.md), con soglie valutate nuovamente e artifact sklearn ricaricati indipendentemente.
