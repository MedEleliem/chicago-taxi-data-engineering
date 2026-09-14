# Chicago Taxi Trips - Data Engineering Project

Ce projet part du notebook Pandas fourni pour le test technique. L'objectif est
de garder une logique facile a lire, puis de la transformer en une chaine data
qui peut etre relancee, controlee et exploitee dans une application web.

La version finale utilise PySpark pour les traitements, Airflow pour
l'orchestration, un stockage compatible S3 pour les donnees, FastAPI pour l'API
et du HTML/CSS/JavaScript simple pour le dashboard.

![Vue principale du dashboard](docs/reports/ui-overview-desktop.png)

## Le premier draft : comprendre le besoin avec Pandas

Le projet n'a pas commence directement avec Airflow, Spark et plusieurs
services Docker. Le premier travail a ete realise dans le notebook
[pandas_only_draft.executed.ipynb](notebooks/pandas_only_draft.executed.ipynb).
Cette version est conservee dans le depot pour montrer le raisonnement avant
l'industrialisation, avec ses cellules executees et ses premiers resultats.

Pandas et Jupyter ont permis d'avancer rapidement sur les questions de base :

- comprendre les colonnes renvoyees par l'API Chicago ;
- filtrer une periode avec `trip_start_timestamp` ;
- convertir les dates et les montants ;
- reperer les valeurs manquantes, negatives ou incoherentes ;
- tester les premiers KPI et regroupements ;
- verifier visuellement les pickups, dropoffs et zones sur une carte ;
- construire une premiere separation Bronze, Silver et Gold.

Ce draft est utile parce qu'il rend l'exploration visible. Une cellule peut etre
modifiee et rejouee immediatement, les DataFrames sont faciles a inspecter et
les regles metier peuvent etre discutees avant de construire l'infrastructure.
Il a servi de specification fonctionnelle pour la suite du projet.

Le notebook montre aussi les limites de cette premiere approche. Toutes les
donnees vivent dans la memoire d'un seul processus, l'ordre d'execution des
cellules peut changer le resultat, et une erreur au milieu demande souvent une
relance manuelle. Il ne fournit pas a lui seul un ordonnanceur, une publication
atomique, un historique de runs, un stockage partage ou une API stable pour le
dashboard.

L'industrialisation a donc ete progressive :

```text
Notebook Pandas
  -> validation du besoin et des regles metier
  -> jobs PySpark separes Bronze / Silver / Gold
  -> quality gates et quarantaine
  -> snapshots versionnes dans S3
  -> orchestration Airflow
  -> API FastAPI et dashboard web
  -> future plateforme MLOps de prediction tarifaire
```

Le notebook reste un premier draft pedagogique. Le code sous `src/` est la
reference executable actuelle. La sortie HTML de la carte a ete conservee dans
le notebook, mais la cle CARTO a ete remplacee par `REDACTED` avant la
publication publique.

## Le point important sur les dates

Le pipeline travaille par journee. Le parametre `processing_date=2023-06-01`
veut dire :

```text
trip_start_timestamp >= 2023-06-01T00:00:00
trip_start_timestamp <  2023-06-02T00:00:00
```

La borne de debut est incluse et celle du lendemain est exclue. Cela evite de
lire deux fois un trajet situe exactement a minuit lorsque plusieurs jours sont
traites.

L'API SODA est lue par pages de 50 000 lignes, avec deux pages maximum dans la
configuration actuelle. Le lot peut donc contenir jusqu'a 100 000 lignes. Le
`2023-06-01`, utilise pour la validation, contient 24 337 lignes : l'extraction
est complete des la premiere page. Si une autre journee depasse la limite,
DataOps bloque la suite car `require_complete_bronze` vaut `true`.

Changer la date dans le dashboard ne lance pas Airflow. Le selecteur sert
seulement a lire une date Gold deja publiee. Pour ajouter une nouvelle date, il
faut declencher le DAG ou lancer le pipeline manuellement, puis actualiser la
page.

## Comment les donnees circulent

```mermaid
flowchart LR
    A[Chicago SODA API] --> B[Bronze PySpark]
    B --> C[Snapshot Bronze S3]
    C --> D[Silver PySpark]
    D --> Q[Quarantine]
    D --> E{Quality gate}
    E -->|PASS ou WARNING| F[Snapshot Silver S3]
    E -->|FAIL| X[Arret du pipeline]
    F --> G[Gold PySpark]
    G --> H[Snapshot Gold S3]
    H --> I[FastAPI]
    I --> J[Dashboard web]
```

### Bronze

Bronze telecharge la journee demandee depuis le dataset Chicago Taxi Trips
`wrvz-psew`. La reponse JSON brute est conservee avec un Parquet contenant
toutes les colonnes en texte. A ce niveau, les valeurs ne sont pas corrigees :
la couche garde ce que la source a fourni et enregistre le nombre de pages, de
lignes et le statut de la pagination.

### Silver

Silver applique le schema PySpark, convertit les timestamps et les nombres,
puis separe les lignes valides des lignes rejetees. Les erreurs sont par exemple
un identifiant absent, une duree negative ou un montant incoherent. Les champs
moins critiques, comme une coordonnee manquante, restent dans Silver avec un
warning.

Silver garde une seule ligne valide par `trip_id`. En cas de doublon, le choix
est deterministe : une ligne sans erreur passe en premier, puis les valeurs sont
comparees dans un ordre stable. Les autres occurrences vont en quarantaine avec
`DUPLICATE_TRIP_ID_EXCLUDED`.

### DataOps

Apres Bronze et Silver, le quality gate controle les manifests, les compteurs,
la reconciliation `input = valid + rejected`, le taux de rejet et la fin de la
pagination. Un lot vide ou incomplet est refuse. Le seuil de warning est 2 % et
le seuil d'echec est 5 %.

### Gold

Gold ne lit que Silver Trusted. Il produit huit tables Parquet :

- `kpi_summary` pour les chiffres globaux ;
- `daily` et `hourly` pour l'evolution dans le temps ;
- `zones`, `companies` et `payments` pour les analyses par dimension ;
- `trips` pour la liste et le detail des trajets ;
- `geo` pour les points et les liaisons affiches sur la carte.

Les lignes de la carte relient les centroides pickup et dropoff. Elles montrent
une origine et une destination, pas la route GPS reellement suivie par le taxi.

## Pourquoi un stockage objet

Les dossiers locaux servent seulement d'espace de travail a Spark. Chaque
couche validee est envoyee dans SeaweedFS avec l'API S3 de boto3. Une publication
a son propre `run_id`, son inventaire de fichiers et ses checksums SHA-256.

Une relance sur la meme date ne detruit pas l'ancien run. Elle cree un nouveau
snapshot, puis remplace le petit pointeur `latest.json` seulement lorsque tous
les fichiers ont ete envoyes et verifies. Si l'upload echoue au milieu, l'ancien
snapshot reste visible.

L'API dispose de son propre cache et recupere Gold depuis S3. Elle ne partage
pas le dossier de travail d'Airflow.

## Lancer le projet avec Docker

Il faut Docker Desktop en mode conteneurs Linux. Depuis la racine du depot :

```powershell
docker compose up --build -d
docker compose ps
```

Les quatre services doivent etre `healthy` :

- `object-store` : stockage S3 SeaweedFS ;
- `postgres` : metadonnees Airflow ;
- `airflow` : orchestration et jobs PySpark ;
- `api` : FastAPI et dashboard.

Au premier lancement, la construction de l'image Airflow peut prendre plusieurs
minutes car elle installe Java et PySpark.

| Service | Adresse |
|---|---|
| Dashboard | http://localhost:18000 |
| Documentation API | http://localhost:18000/docs |
| Airflow | http://localhost:18080 |
| Stockage | http://localhost:18888 |
| Endpoint S3 | http://localhost:18333 |

Airflow utilise les identifiants locaux `artefact / chicago-local`.

## Lancer une date

Pour la date par defaut `2023-06-01` :

```powershell
docker compose exec airflow airflow dags trigger chicago_taxi_batch_pipeline
docker compose logs -f airflow
```

Pour une autre date, l'utilisateur peut ouvrir le formulaire **Trigger DAG**
dans Airflow et remplir `processing_date`. La chaine peut aussi etre executee
sans le DAG :

```powershell
docker compose exec airflow python -m scripts.run_pipeline --processing-date 2023-06-02
```

Pour lancer les couches une par une :

```powershell
docker compose exec airflow python -m scripts.run_stage --layer bronze --processing-date 2023-06-02
docker compose exec airflow python -m scripts.run_stage --layer silver --processing-date 2023-06-02
docker compose exec airflow python -m scripts.run_stage --layer gold --processing-date 2023-06-02
```

Deux traitements ne doivent pas etre lances en meme temps sur la meme date, car
ils partagent encore un espace de travail local.

Pour arreter les services sans supprimer les donnees :

```powershell
docker compose stop
```

`docker compose down -v` supprime les volumes. Cette commande est donc a eviter
si les snapshots et l'historique Airflow doivent etre conserves.

## Ce que montre le dashboard

Le dashboard contient quatre vues :

- **Overview** : KPI et tendances principales ;
- **Operations** : volumes horaires, paiements, entreprises et liste des trips ;
- **Geography** : pickups, revenus par zone et detail d'un trajet sur la carte ;
- **Data Quality** : lignes valides, rejets, warnings et erreurs detectees.

Les filtres de date et d'entreprise sont envoyes a FastAPI. L'API lit les
tables Gold correspondantes et renvoie du JSON. Spark n'est jamais lance par
une requete web. Foursquare est facultatif et sert uniquement a chercher des
lieux proches d'un point selectionne.

La cle CARTO est lue depuis `.env.local`, qui est ignore par Git et exclu des
images Docker. `.env.example` montre seulement les noms des variables attendues.

## Pourquoi le dashboard actuel reste une demonstration

Le dashboard Compose utilise de vraies donnees historiques de Chicago. Les
chiffres du `2023-06-01` ne sont donc pas generes artificiellement. Le projet
reste cependant une demonstration pour trois raisons :

- l'acquisition est declenchee manuellement, journee par journee ;
- la source SODA ne pousse pas les nouvelles courses sous forme d'evenements ;
- le dashboard lit la derniere publication Gold validee, pas un flux continu.

Une date affichee represente ainsi un snapshot controle et reproductible. Elle
ne correspond pas a l'etat des taxis a la seconde presente. Un mode synthetique
separe existe avec `python -m scripts.build_demo` pour tester l'interface sans
appeler Chicago, mais il n'est pas utilise dans les resultats d'acceptation.

Cette V1 sert d'abord a fiabiliser le socle : ingestion, qualite, lineage,
idempotence, stockage et exposition API. Une prediction temps reel n'est utile
que si les donnees qui alimentent le modele sont elles-memes fiables.

## Exploitation cible du dashboard en temps reel

L'ambition est de faire evoluer cette interface d'un outil d'analyse historique
vers un produit utilisable avant, pendant et apres une course.

Pour un passager, une future vue **Fare Estimate** permettrait de renseigner un
point de depart, une destination et une heure souhaitee. L'application
calculerait les zones, une distance et une duree estimees, puis appellerait une
API de prediction pour afficher un prix attendu et un intervalle de confiance.

Pour une equipe operationnelle, le dashboard pourrait suivre :

- le volume de demandes, les pickups et le revenu presque en temps reel ;
- les zones ou la demande augmente ;
- le prix predit compare au prix observe a la fin de la course ;
- les erreurs du modele par zone, heure et compagnie ;
- la derive des variables et la version du modele actuellement servie.

Le flux cible serait le suivant :

```mermaid
flowchart LR
    A[Course demandee] --> B[Features temps reel]
    B --> C[API de prediction]
    C --> D[Prix estime + intervalle]
    D --> E[Dashboard utilisateur]
    F[Course terminee] --> G[Prix reel]
    G --> H[Monitoring et derive]
    H --> I[Donnees de reentrainement]
```

Le dashboard ne doit pas lancer un reentrainement a chaque requete. Il appelle
uniquement un service de prediction leger. L'entrainement, les controles et la
mise en production restent dans une plateforme separee.

## Ambition MLOps : predire le prix d'une course

La prochaine grande etape est un modele de regression capable d'estimer le
`fare` avant le depart. Le prix total avec pourboire ne serait pas une bonne
cible avant la course, car le pourboire n'est connu qu'apres le paiement.

Les premieres variables candidates sont la zone de depart, la zone d'arrivee,
l'heure, le jour de la semaine, la distance et la duree estimees. Des donnees de
trafic, meteo, evenements ou niveau de demande pourraient ensuite completer ces
features si leur disponibilite temps reel est garantie. Les variables connues
seulement apres la course doivent etre exclues pour eviter la fuite de cible.

L'architecture MLOps vise le meme principe que le projet public
[FraudOps MLOps Demo](https://github.com/MedEleliem/fraudops-mlops-demo) :

```text
Pipeline training / admin
  -> construction du dataset et des features
  -> entrainement et evaluation de plusieurs candidats
  -> suivi des experiences avec MLflow
  -> versionnement des donnees et artefacts avec DVC
  -> comparaison avec le modele de production
  -> validation puis promotion dans le registry

Service de prediction
  -> charge uniquement le modele approuve
  -> expose /predict, /health et /model-info
  -> repond rapidement sans Spark, DVC ou MLflow
  -> journalise prediction, latence et version du modele
```

Chaque modele devra avoir une version immuable, ses parametres, ses metriques,
son schema d'entree et la periode de donnees utilisee. Une CI pourra bloquer la
promotion si les erreurs globales ou les erreurs par zone regressent. Une image
Docker de serving sera ensuite associee a la version approuvee.

Cette partie est une roadmap. La V1 actuelle fournit les donnees Gold, l'API et
les controles necessaires pour la preparer, mais elle n'entraine et ne sert
encore aucun modele de prediction de prix.

## Tests et resultats obtenus

La suite complete dans l'image Linux finale donne :

```text
37 passed, 1 skipped
```

Le test ignore par defaut est l'integration S3 destructive. Il a ete execute
separement contre le stockage Compose et il passe. Le controle navigateur
contient 19 checks : graphiques, filtres, pagination, carte, tuiles CARTO,
details des trajets, qualite, responsive mobile et absence d'erreur JavaScript.

Pour relancer les tests :

```powershell
docker compose exec -T -e S3_ENDPOINT_URL= airflow python -m pytest tests -q -p no:cacheprovider

$env:CHICAGO_TEST_URL="http://127.0.0.1:18000"
python -m scripts.check_frontend

python scripts/check_release.py --processing-date 2023-06-01 --runs 2
```

`check_release.py` declenche deux DAGs sur la meme date et compare les KPI, le
nombre d'identifiants uniques et le hash du contenu Gold. Les deux runs valides
ont produit 24 337 lignes Bronze, 23 882 lignes Gold et 455 rejets, avec le meme
hash metier mais de nouveaux `run_id`.

Les preuves sont dans [docs/reports](docs/reports) et le bilan detaille est dans
[docs/reports/livraison.md](docs/reports/livraison.md).

## Organisation du depot

```text
config/                 configuration et seuils
dags/                   DAG Airflow
docker/                 images Airflow et API
frontend/               HTML, CSS et JavaScript
notebooks/              premier draft Pandas execute
scripts/                lancement, validation et tests de release
src/bronze/             ingestion SODA
src/silver/             typage, controles et quarantaine
src/gold/               tables analytiques
src/dataops/            quality gates et audits
src/api/                FastAPI
src/object_store.py     publication et restauration S3
tests/                  tests unitaires et integration
docs/                   explications et preuves
```

Pour comprendre le code dans l'ordre, le parcours conseille est de lire
[l'architecture](docs/architecture.md), puis [Bronze](docs/bronze.md),
[Silver](docs/silver.md), [Gold](docs/gold.md), [le stockage](docs/storage.md),
[Airflow](docs/airflow.md) et enfin [l'API](docs/api.md) avec
[le frontend](docs/frontend.md).

## Choix et limites

Le projet utilise des modules Python simples plutot qu'un framework
supplementaire. Le DAG orchestre des commandes mais ne contient pas les
transformations. Cette separation permet de tester chaque couche sans demarrer
Airflow.

Cette version reste adaptee a un test local et a une fenetre journaliere :

- Bronze accumule la reponse du jour en memoire avant de creer le DataFrame ;
- l'API charge les trajets Gold en RAM ;
- les montants utilisent `DOUBLE`, comme dans le notebook de reference ;
- les timestamps ne sont pas convertis vers un autre fuseau ;
- les assets web et les tuiles cartographiques demandent Internet ;
- il n'y a pas encore de verrou distribue entre deux pipelines concurrents.

Pour aller plus loin, les prochaines evolutions data seraient une lecture S3A
directe dans Spark, un format transactionnel, une extraction SODA en streaming,
une politique de retention, un moteur SQL analytique et une CI qui reconstruit
Compose et rejoue les tests automatiquement.
