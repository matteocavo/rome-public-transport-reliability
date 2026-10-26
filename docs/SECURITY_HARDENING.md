# Hardening Git e CI

Credenziali locali, cache, export reali, modelli, pickle e log sono esclusi da Git e
rifiutati dal controllo dei file tracked in CI. I deliverable PDF/immagini del core
rimangono ammessi. Nessuna scansione stampa valori di secret.

Actions checkout/setup-python sono fissate a SHA completi dei repository ufficiali;
contents: read esplicito e persist-credentials: false. Non viene introdotto pull_request_target.
Dependabot verifica GitHub Actions settimanalmente; nell’app verifica anche pip tramite
requirements-dev.txt e pyproject.toml. requirements-dev.lock rimane un entry point compatibile.
Il core usa librerie gestite dal runtime Databricks e non aggiunge un manifest pip artificiale.

Alert e security updates sono impostazioni remote separate dal file Dependabot.
La policy remota può limitare le action ai due repository necessari. L’obbligo globale
SHA deve essere attivato solo dopo che i workflow dei branch mantenuti sono aggiornati,
per evitare di bloccare le PR ancora basate sui vecchi tag.
Code scanning rimane rinviato esplicitamente; nessun merge automatico degli aggiornamenti.
