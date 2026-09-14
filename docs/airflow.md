# Orchestration Airflow

Les transformations restent dans `src/`. Airflow orchestre uniquement les commandes, les retries et les dépendances définis dans `dags/chicago_taxi_batch_pipeline.py`.

## DAG journalier

`chicago_taxi_batch_pipeline` reçoit une `processing_date` au format ISO:

```text
start -> bronze -> validate_bronze -> silver -> quality_gate
      -> approve_data_and_business_quality -> gold -> validate_gold -> end
```

Chaque étape de traitement appelle `scripts.run_stage`: restauration de la source depuis S3, exécution PySpark, contrôle DataOps puis publication du snapshot. Un Quality Gate en échec empêche Gold de démarrer.

Après les contrôles automatiques, `ApprovalOperator` met le run en attente. Le
reviewer ouvre **Required Actions** dans Airflow, vérifie les compteurs, le taux
de rejet et la cohérence métier, puis choisit **Approve** ou **Reject**. Approve
continue vers Gold; Reject ignore les tâches suivantes. Le délai maximal de
réponse est de sept jours.

Le DAG accepte un seul run actif. Cette contrainte protège le pointeur S3 `latest` et les partitions locales contre des écritures concurrentes.

## DAG de plage

`chicago_taxi_range_pipeline` reçoit `start_date` et `end_date`. La fonction partagée `src.date_range.processing_dates` valide les bornes et génère toutes les dates incluses. Airflow Dynamic Task Mapping crée ensuite un `TriggerDagRunOperator` par journée.

Le contrôleur attend la fin de chaque run. Les opérateurs sont différables et le DAG journalier reste sérialisé. Une plage est limitée à 92 jours.

Chaque partition déclenchée par le contrôleur demande sa propre décision
humaine. Pour un backfill non supervisé, `scripts.run_date_range` conserve les
quality gates automatiques mais n'utilise pas l'interface HITL.

Exemple de configuration:

```json
{"start_date": "2023-06-01", "end_date": "2023-06-07"}
```

Ces DAGs sont manuels (`schedule=None`). Changer un champ dans l'interface ne déclenche pas automatiquement un run.

## Commandes

```powershell
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags trigger chicago_taxi_batch_pipeline
docker compose exec airflow airflow dags trigger chicago_taxi_range_pipeline
docker compose logs -f airflow
```

Pour choisir les dates, utiliser **Trigger DAG w/ config** dans l'interface Airflow. Cela évite les différences d'échappement JSON entre PowerShell et Bash.

Une relance d'une journée crée de nouveaux `run_id` techniques, puis remplace la partition métier seulement après validation. Les snapshots S3 restent immuables par `run_id`.
