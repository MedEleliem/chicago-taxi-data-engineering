# Rapport de livraison

## Perimetre

La reference initiale est `notebooks/pandas_only_draft.executed.ipynb`. La
livraison executable est isolee dans `chicago_taxi_app` et ne depend ni du
notebook ni de Graphify. Le graphe Graphify global reste un outil de lecture de
l'architecture du projet d'origine.

## Correspondance avec le test

1. **Source** : extraction SODA bornee par `trip_start_timestamp`, ordonnee et
   paginee. La date reproductible validee est `2023-06-01`.
2. **Bronze** : JSON source brut, Parquet tout-string, manifeste et metadonnees
   de pagination.
3. **Silver** : schema type PySpark, controles bloquants, warnings, quarantaine
   et deduplication deterministe par `trip_id`.
4. **Gold** : huit tables Parquet, controles de reconciliation et KPI.
5. **Stockage objet** : snapshots S3 immuables par `run_id`, inventaires SHA-256
   et pointeur `latest.json` publie conditionnellement.
6. **Orchestration** : un DAG Airflow de huit taches. Le DAG orchestre les
   commandes; les transformations restent dans `src`.
7. **Restitution** : API FastAPI et application HTML/CSS/JavaScript avec Plotly,
   MapLibre et fond CARTO configure hors du code source.
8. **Execution** : SeaweedFS S3, PostgreSQL, Airflow et API demarrent avec
   `docker compose up --build -d`.

## Acceptation reelle

Le 14 septembre 2026, deux DAGs complets ont traite independamment la journee
`2023-06-01`. Les deux sont `success` :

- 24 337 lignes Bronze ;
- 23 882 lignes Silver valides et Gold ;
- 455 lignes rejetees, soit 1,8696 % ;
- pagination source complete ;
- 23 882 `trip_id` Gold distincts ;
- 691 745,98 de revenu total et 2 339 taxis distincts ;
- meme SHA-256 metier sur les deux runs :
  `65e10d5d4245920656f626621f17740ea09a1e7d361cabc9d2ed399308e8b4cf` ;
- nouveaux identifiants Bronze, Silver et Gold a chaque relance ;
- 50 objets verifies dans chaque publication Gold.

Toutes les assertions de `compose-acceptance.json` sont vraies : reconciliation,
pagination, unicite, idempotence des KPI et du contenu, nouveaux run IDs, API et
page HTML. Le test S3 reel contre SeaweedFS passe et couvre publication
conditionnelle, telechargement et checksum. Les tests unitaires couvrent aussi
une interruption d'upload : l'ancien pointeur reste alors visible.

Le controle navigateur `frontend-checks.json` contient 19 PASS : KPI,
graphiques, filtres, pagination, details, carte et tuiles, modes geographiques,
qualite, comportement Foursquare facultatif, absence d'erreur JavaScript et de
debordement a 390 px et 768 px. Les captures `ui-*.png` sont jointes.

La suite complete executee dans l'image Linux finale donne `37 passed, 1
skipped` (`pytest-final.xml`). Le skip est le test S3 destructif optionnel, deja
execute separement avec succes contre SeaweedFS (`s3-integration.xml`). Une
execution Windows a donne 36 PASS avant une deconnexion transitoire d'un worker
PySpark au stage 191 ; aucune assertion metier n'a echoue et l'environnement de
livraison Docker ne reproduit pas cet incident.

## Commandes de preuve

```powershell
docker compose up --build -d
python scripts/check_release.py --processing-date 2023-06-01 --runs 2
$env:CHICAGO_TEST_URL="http://127.0.0.1:18000"
python -m scripts.check_frontend
python -m pytest tests -q
```

## Limites assumees

Bronze accumule la fenetre journaliere en memoire Python avant Spark. L'API
charge Gold en RAM. Spark utilise un disque de travail local avant publication
S3. Le Compose local limite Airflow a une tache concurrente et peut etre lent
sur une machine peu dotee. Les lignes cartographiques relient des centroides et
ne representent pas les routes GPS. Foursquare est facultatif. Les assets web et
les tuiles demandent Internet.

Avec plus de temps : lecture Spark S3A directe, format transactionnel, retention
automatisee, verrous entre executions concurrentes, extraction SODA en flux,
moteur SQL analytique, CI et gestion centralisee des secrets.

## Parcours de lecture

1. `docs/architecture.md`
2. `docs/bronze.md`, `docs/silver.md`, `docs/gold.md`
3. `docs/storage.md` et `docs/dataops.md`
4. `docs/airflow.md` et `docs/docker.md`
5. `docs/api.md` et `docs/frontend.md`
6. `docs/reports/code_complet.md`

Les fichiers executables restent la reference. `code_complet.md` est une annexe
regeneree par `python -m scripts.export_code_report` sans inclure de secret.
