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
