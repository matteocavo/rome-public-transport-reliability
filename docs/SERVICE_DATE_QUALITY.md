# Qualità delle service date nel refresh Silver

## Diagnosi del 28 ottobre 2026

Main iniziale: `b4aab1876b6915892c4acc8f20129c2727603ce5`.
Il run downstream `183696913087918` fallisce in 06, sezione E:
`trip_updates: invalid service date; write blocked`. Anche i retry automatici
falliscono; 07/08/13 vengono saltati. Non è un errore quota o timezone.

La diagnosi read-only di Bronze trova una sola osservazione con `start_date=""`,
feed `1793176046`, trip `100915495LA66905`, entity `1351`, stop `31009`,
stop_sequence 14, schedule_relationship SCHEDULED. Route e start_time sono vuoti.
Nella lettura diagnostica non risultano altri valori null, malformati o impossibili.
Il trip compare nel feed precedente con `20261028`, ma il calendario statico ha
52 date attive dal 24 settembre al 4 dicembre: non prova da solo l'istanza del trip
con data mancante. L'osservazione è quindi non recuperabile con certezza e va
esclusa dai derivati; Bronze resta intatto e permette l'audit/riprocessamento.

## Regola di dominio

GTFS-RT start_date identifica la data di servizio, non la data UTC del feed.
Il campo è condizionalmente richiesto e non va sintetizzato dal feed timestamp:
rollover, corse oltre mezzanotte e ritardi rendono tale fallback ambiguo.
Fonti: https://gtfs.org/documentation/realtime/reference/#message-tripdescriptor
 e https://gtfs.org/documentation/schedule/reference/#calendartxt.

- YYYYMMDD valido: invariato.
- Soli spazi ASCII attorno a YYYYMMDD valido: normalizzazione deterministica,
  contata come repaired; nessun cambiamento della data.
- Null, stringa vuota, formato errato o data impossibile: excluded, mai inferita.
- VehiclePosition conserva la semantica originale di data opzionale.

## Budget esplicito e fail-closed

Il budget massimo di esclusione è **0,1%**, globale **e per ogni snapshot**:
si conserva almeno il 99,9% delle osservazioni datate in ciascun feed. È una
scelta di qualità esplicita e conservativa (al massimo una esclusione ogni mille
osservazioni), non un requisito GTFS, un SLA dimostrato o una soglia appresa.
Evita che una singola anomalia storica blocchi tutti i rebuild, ma impedisce che
la storia accumulata diluisca un degrado del feed corrente. Snapshot piccoli con
una perdita superiore al budget, snapshot interamente invalidi, output vuoti,
perdita dell'ultimo epoch, conflitti grain/schema e date invalide residue restano
bloccanti. Una modifica del budget richiede code review; non è un parametro Job.

Le metriche stampano total, valid (escluse le riparate), repaired, invalid/excluded,
percentuale globale e massima per snapshot, limite e risultato del gate.
I campioni sono limitati a dieci valori di data, ciascuno troncato a 32 caratteri;
non si stampano payload o credenziali. Nessuna tabella di quarantena aggiunta:
l'esclusione è logica e la sorgente Bronze versionata conserva la riga originale.

Tutti i controlli precedono le scritture. Lo schema Silver e la pipeline
06 → 07 → 08 → 13 restano invariati. Nessuna modifica a schedule, modello,
training, serving o permessi.

## Validazione runtime completata il 28 ottobre 2026

PR #16, codice distribuito `70299c7c25830cc4db5db109d9ab4a1573b4ac9b`.
Un solo run manuale di validazione `59520050158031`, trigger ONE_TIME:
**SUCCESS**, tutti i task al primo tentativo, nessun retry manuale o automatico.
Questa esecuzione non costituisce una prova della schedule automatica.

| Task | Risultato | Durata start/end (secondi) |
| --- | --- | ---: |
| silver_06 | SUCCESS | 1494.872 |
| enrichment_07 | SUCCESS | 850.341 |
| scoring_input_08 | SUCCESS | 363.082 |
| operational_gold_13 | SUCCESS | 385.236 |

Durata totale Job: 3095.335 secondi. Nessun errore quota/platform
riportato dai task; nessun altro run downstream attivo al controllo finale.
La configurazione Job prima/dopo coincide esattamente: trigger già UNPAUSED ogni
due ore, nessuna modifica a schedule, parametri, permessi o compute.

Input 06: Bronze trip updates v4170, 61.816.631 righe raw, 61.580.451 dopo
la deduplicazione già esistente. Valid date rows: 61.580.450; repaired: 0;
excluded: 1. Invalid globale: 0,000001624%; worst snapshot: 0,003579739%
(una riga su 27.935). La riga esclusa non è presente nella tabella Silver persistita.
Bronze resta intatto. Silver trip updates v26, vehicle positions v21, observations v30;
enrichment v31. Matching: zero duplicati, zero posizioni future, massimo 180 secondi.
Enrichment conserva tutte le 61.580.450 osservazioni valide.

Lettura finale: **2026-10-28T13:04:22.007830+00:00**.

| Layer | Watermark | Righe | Date mancanti/incoerenti |
| --- | --- | ---: | ---: |
| bronze_current | 2026-10-28 12:57:36 UTC | 61932814 | n/a (raw) |
| silver_trip_updates | 2026-10-28 12:07:40 UTC | 61580450 | 0 |
| silver_observations | 2026-10-28 12:07:40 UTC | 61580450 | 0 |
| enrichment | 2026-10-28 12:07:40 UTC | 61580450 | 0 |
| scoring | 2026-10-28 12:07:40 UTC | 21867 | 0 |

Lag Bronze corrente → enrichment: **2996 secondi**; enrichment → scoring: **0 secondi**.
Età scoring al controllo: circa **3402 secondi**. Il watermark scoring avanza
da 07:47:57 a 12:07:40 UTC. È il batch dei sorgenti pinning iniziale, non il wall clock
di fine Job: il rebuild ha richiesto circa 52 minuti mentre ingestion ha continuato.
Il refresh batch è ripristinato; un SLA di freshness di 30 minuti non è dimostrato
dalla cadenza downstream esistente di due ore e non viene promesso.

Scoring input: **21.867 righe**, service_date e target_service_date presenti e coerenti;
read-back validato da 08. Feature contract hash invariato:
`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`.
Gold: quality_status **PASSED**, dashboard_ready **YES**, refresh
**2026-10-28T13:02:20.635Z**. Manifest pubblicato dal task 13 dopo il read-back.

Validazione repository: **60 test PASS** (55 esistenti + 5 nuovi metodi con sottocasi),
compileall PASS, policy file/segreti PASS, git diff --check PASS, CI verde sul fix.
Nessun training/model threshold modificato. Backend, Render, Neon, n8n e workflow
Actions invariati; nessuno scheduler abilitato. PR aperta, nessun merge.

**DATABRICKS FRESHNESS RESTORED — READY FOR POST-FIX REVIEW**
