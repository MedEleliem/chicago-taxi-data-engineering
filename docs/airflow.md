# Airflow : un seul DAG, aucune transformation

Fichier : `dags/chicago_taxi_batch_pipeline.py`. Cible : Airflow 3.2.1 avec le
provider standard. Le DAG importe `DAG` et `Param` depuis `airflow.sdk`, puis
les operateurs Bash et Empty du provider standard.

## Lecture du code

Le contexte `with DAG(...)` fixe l'identifiant, le parametre processing_date,
les delais et les limites de concurrence. schedule=None signifie lancement
manuel, pas de collecte automatique. catchup=False evite les lots historiques
automatiques. max_active_runs=1 serialise les executions de ce DAG.

Le dictionnaire commands est la liste ordonnee des six vraies commandes.
Les trois traitements appellent desormais scripts.run_stage, qui restaure
la source S3, execute le job, controle sa sortie et publie le snapshot.
La boucle cree un BashOperator par commande et le relie au precedent :

```text
start -> bronze -> validate_bronze -> silver -> quality_gate
      -> gold -> validate_gold -> end
```

Le parametre date est transmis par variable d'environnement et cite dans Bash,
pas injecte dans le texte executable. Le schema Param et partition_path
verifient le format et la date. CHICAGO_PROJECT_ROOT selectionne la racine de
travail Linux ; CHICAGO_CONFIG selectionne le YAML. Les transformations restent
dans src/bronze, src/silver et src/gold.

Chaque tache peut etre retentee une fois, apres une minute avec backoff borne.
Ces retries relancent une commande complete ; les retries HTTP Bronze ne
relancent que la page en erreur. Les run_id metier ne contiennent pas try_number.
Une nouvelle tentative produit un nouveau manifest et remplace uniquement la
partition de la date apres validation. Une tache gate en FAIL termine avec le
code 1 : avec la dependance all_success par defaut, Gold ne demarre pas.

## Verification attendue sous Linux

Airflow n'est pas une dependance du serveur FastAPI ni des jobs standalone.
Dans un environnement Airflow provisionne avec Java, PySpark et le projet :

```bash
export CHICAGO_PROJECT_ROOT=/chemin/chicago_taxi_app
export AIRFLOW__CORE__DAGS_FOLDER="$CHICAGO_PROJECT_ROOT/dags"
airflow db migrate
airflow dags list-import-errors
airflow dags test chicago_taxi_batch_pipeline --conf '{"processing_date":"2023-06-01"}'
airflow dags trigger chicago_taxi_batch_pipeline --conf '{"processing_date":"2023-06-01"}'
```

Le test execute de vrais jobs et contacte Chicago : il peut durer plusieurs
minutes. Ne pas lancer en parallele un job manuel sur la meme date.
Une execution reussie dans Airflow est necessaire avant de declarer cette etape
validee. Une compilation Python seule ne constitue pas cette preuve.

L'environnement de livraison est maintenant [Docker Compose](docker.md), avec
PostgreSQL et les services Airflow geres par la commande officielle standalone.

Reference : [documentation officielle du test de DAG](https://airflow.apache.org/docs/apache-airflow/3.2.1/core-concepts/debug.html).
