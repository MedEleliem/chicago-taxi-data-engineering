# Docker Compose : lancement reproductible du test

## Services et choix

Quatre services suffisent : `airflow` (mode standalone avec LocalExecutor),
`postgres` (metadonnees Airflow), `object-store` (SeaweedFS compatible S3),
`api` (FastAPI et frontend). Spark est local au conteneur Airflow. Le mode
standalone rassemble les processus officiels Airflow dans un seul conteneur :
il convient a ce test local, pas a un deploiement distribue de production.

Le stockage objet est le socle persistant des trois couches. Les dossiers
locaux du job sont un espace de travail. Avant Silver et Gold, run_stage
restaure explicitement la publication source depuis S3. L'API a un volume
cache separe et ne partage pas le disque du pipeline : elle lit uniquement
les snapshots Gold et leurs rapports de provenance depuis S3.

SeaweedFS 4.46 fournit le mode mono-conteneur mini. Le choix evite d'ajouter un
cluster de stockage ou de compiler MinIO pour ce test. Les interfaces S3 restent
standard : boto3 peut pointer vers un autre serveur compatible.
References : [version officielle](https://github.com/seaweedfs/seaweedfs/releases/tag/4.46)
et [mode mini](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini).

## Demarrer

Depuis chicago_taxi_app (ou depuis la racine de developpement) :

```powershell
docker compose up --build -d
docker compose ps
```

Au premier lancement, le telechargement et la construction de PySpark peuvent
prendre plusieurs minutes. Docker Desktop doit fonctionner en conteneurs Linux.
Prevoir suffisamment de RAM disponible ; le conteneur Airflow est limite a 3 Go.

- Dashboard : http://localhost:18000
- API : http://localhost:18000/docs
- Airflow : http://localhost:18080 (artefact / chicago-local)
- Explorateur de stockage : http://localhost:18888
- Endpoint S3 : http://localhost:18333

Les ports sont lies a 127.0.0.1 et personnalisables avec CHICAGO_API_PORT,
CHICAGO_AIRFLOW_PORT, CHICAGO_S3_PORT et CHICAGO_STORAGE_UI_PORT.
PostgreSQL n'est pas expose sur la machine hote.

## Declencher le vrai pipeline

Dans Airflow, ouvrir chicago_taxi_batch_pipeline, puis Trigger et choisir
processing_date. La date de demonstration reelle par defaut est 2023-06-01.
Ou, avec cette date par defaut :

```powershell
docker compose exec airflow airflow dags trigger chicago_taxi_batch_pipeline
docker compose logs -f airflow
```

Pour choisir une autre date, utiliser le formulaire Airflow (cela evite les
differences de guillemets JSON entre PowerShell et Bash). Le DAG n'a pas de
planification automatique : un lot ne demarre pas sans declenchement.

Jobs manuels dans l'environnement provisionne, sans orchestration :

```powershell
docker compose exec airflow python -m scripts.run_pipeline --processing-date 2023-06-01
docker compose exec airflow python -m scripts.run_stage --layer bronze --processing-date 2023-06-01
docker compose exec airflow python -m scripts.run_stage --layer silver --processing-date 2023-06-01
docker compose exec airflow python -m scripts.run_stage --layer gold --processing-date 2023-06-01
```

Ne pas lancer les commandes manuelles pendant un DAG sur la meme date.
Un run_stage inclut son controle DataOps et sa publication S3. L'appel direct
a src.gold ou src.silver reste un outil local qui ne publie pas dans S3.

## Persistance et arret

Les objets et metadonnees persistent dans object-data et postgres-data.
airflow-home conserve la configuration et les logs Airflow. Les Parquets de
travail, rapports et logs du pipeline sont sous data/docker sur l'hote.
api-cache contient uniquement des snapshots telecharges, recreables depuis S3.

```powershell
docker compose stop
docker compose start
docker compose down
```

Ne pas ajouter `-v` a down : cela supprimerait les volumes persistants.
Les services et volumes des autres projets Docker ne sont pas modifies.

## Secrets et limites

Les valeurs par defaut sont des identifiants DE DEVELOPPEMENT LOCAL, pas des
secrets de production. Pour les remplacer avant la premiere initialisation,
definir CHICAGO_S3_USER, CHICAGO_S3_PASSWORD, CHICAGO_POSTGRES_PASSWORD,
CHICAGO_JWT_SECRET et CHICAGO_AIRFLOW_PASSWORD dans un fichier .env ignore.
Un changement de mot de passe ne reinitialise pas une base existante.

.env.local reste optionnel et n'est lu que par api pour CARTO/Foursquare.
.dockerignore utilise une liste d'inclusion : ni .env, ni donnees, ni notebook,
ni cles locales ne sont envoyes dans le contexte de construction.

Pour la production : comptes S3 en moindre privilege, TLS, secrets geres,
images analysees et mises a jour, sauvegardes et supervision sont necessaires.
L'objectif ici reste un test local reproductible, pas une plateforme hautement disponible.
