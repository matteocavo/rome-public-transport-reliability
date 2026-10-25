# Decisioni architetturali

## ADR-001: ML operativo esterno a Databricks Free Edition

**Stato: accettata il 25 ottobre 2026.** Target ML operativo: **backend Python esterno**.
Stato Spark ML: **ML storico / sperimentale - implementazione di ricerca / riferimento**.
Questa decisione sostituisce il percorso di deployment operativo proposto in PR4; non invalida
gli esperimenti storici, le metriche TEST o le previsioni del replay.

### Evidenze e decisione

Il workspace è Free Edition e dispone solo di compute serverless. Nella sessione Spark Connect
osservata non è stato possibile costruire `VectorAssembler`: lo smoke test minimo di
serializzazione si è fermato con `Py4JSecurityException` (whitelist dei costruttori), prima
del fit o della serializzazione. Impostare esplicitamente l'URI di tracking MLflow
`databricks` e quello del registro `databricks-uc` ha consentito selezione dell'esperimento,
creazione della run e logging dei parametri. Il tracking riuscito non ha dimostrato la
persistenza del modello. È passato anche un test di scrittura isolata su Volume.

L'unica richiesta autorizzata di creazione di compute Dedicated 16.4 LTS ML CPU è stata rifiutata:
`Current organization ... does not have any associated worker environments`.
Non è stato creato alcun cluster classic, non è stato eseguito alcun fit operativo e nessun
artifact operativo PR4 è stato persistito o ricaricato. Nessun compute di quel tentativo è
rimasto attivo. Gli esperimenti storici Spark/MLflow e le evidenze dello smoke test fallito
sono conservati al di fuori dei dump di debug pubblici.

Scegliamo un confine del runtime che non richieda compute classic a pagamento: Databricks
prepara ed espone contratti dati versionati; un futuro backend FastAPI/Python/scikit-learn
gestisce training, valutazione, persistenza, caricamento e inference del modello operativo.
Non è prevista la conversione automatica della Random Forest Spark storica. Algoritmi,
preprocessing, calibrazione, iperparametri e soglie di probabilità richiedono una nuova
valutazione temporale. Questa PR fornisce contratti e generatori SQL, non un backend in esecuzione.

### Responsabilità

| Componente | Responsabilità |
| --- | --- |
| Questo repository Databricks | Ingestion GTFS, Delta Bronze/Silver, feature causali, etichette, analytics, Gold, contratti dati e metadati sulla freschezza dei dati (freshness) |
| Futuro repository `rome-mobility-intelligence` | FastAPI, training/valutazione sklearn, artifact completi del modello e configurazione, versioni immutabili del modello, caricamento, inference e API |
| n8n, nel futuro repository | Pianificazione tra sistemi, controlli di readiness, invocazione del backend, retry limitati, alert e definizioni/export dei workflow |
| React + Google AI Studio | Frontend dell'app; Google AI Studio genera e itera la UX/UI, React consuma le API e mostra provenienza e freshness |
| Databricks AI/BI / Power BI | Analytics del lakehouse; Power BI resta una presentazione separata pianificata, non l'app |

Questa PR non crea né inizializza il futuro repository dell'app. La documentazione pubblica
usa il nome del repository anziché il percorso locale del filesystem dello sviluppatore.

### Conseguenze e criteri di accettazione

I notebook storici 09-12 e gli helper/chiamanti PR4 restano disponibili con logica originale
e gate PR4 disabilitati. I loro contratti specifici Spark sono evidenze di riferimento,
non istruzioni per il futuro deployment operativo. La preparazione condivisa dei dati
osservabili resta la sorgente delle 47 feature grezze. L'hash esistente include impostazioni
di preprocessing storiche: va mantenuto come provenienza degli input, mentre il futuro
artifact sklearn avrà una propria versione di preprocessing/artifact. Non si dichiara
equivalenza tra i modelli Spark e sklearn.

Nessun componente fondamentale del target richiede persistenza Spark ML o registrazione
del modello in UC. L'implementazione resta futura, con criteri di accettazione espliciti,
senza dichiarare un funzionamento end-to-end attuale. Le quote Free Edition e l'hosting
del backend esterno restano vincoli di disponibilità; non esistono uno SLA commerciale
o una promessa di servizio live continuo. Prima di qualsiasi dichiarazione operativa live
devono passare i controlli su dati downstream aggiornati, nuovo modello valutato,
ricaricamento/parità dell'artifact persistito e inference indipendente.

Vedi [contratti di handoff, accesso, misure e rollout](OPERATIONAL_ML_HANDOFF.md),
[roadmap](ROADMAP.md) e [audit dashboard](../dashboard/databricks/README.md).

### Fonti

- [Limiti Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations): solo compute serverless, un SQL warehouse piccolo, quote di utilizzo equo e nessuno SLA.
- [Statement Execution API](https://docs.databricks.com/api/statement-execution/v1/statement-execution): accesso SQL supportato e formati dei risultati suddivisi in chunk.
- Le osservazioni runtime controllate sopra riportate sono state registrate il 25 ottobre
  2026; descrivono questo workspace, non un limite universale di tutti i deployment Spark ML.
