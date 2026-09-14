# Bronze PySpark, explique pas a pas

## Objectif et source

Le job transforme les enregistrements JSON recus de l'API SODA Chicago en un
dataset Parquet local. Il ne retire aucun trajet et n'applique pas les regles
metier de Silver. Son role est de conserver une entree, de mesurer ses manques
et de tracer l'execution.

La source configuree est `data.cityofchicago.org`, dataset `wrvz-psew`, repris
du projet et du notebook de reference. Le job utilise l'endpoint
`https://<domain>/resource/<dataset_id>.json`. Un token peut etre fourni dans
la variable d'environnement nommee par `app_token_env` ; sa valeur n'est ni
dans le YAML ni ecrite dans le manifest.

Source executable : [src/bronze/__main__.py](../src/bronze/__main__.py).
Le code est compose de fonctions. Aucun client abstrait, factory ou framework
d'injection n'est requis. `requests` sert au transport HTTP ; Spark traite et
ecrit les donnees. Le notebook reste la specification fonctionnelle.

## Choisir une journee

```powershell
python -m src.bronze --processing-date 2023-06-01
```

`build_date_window` verifie la date puis ajoute un jour avec `timedelta`.
Ce calcul gere naturellement la fin d'un mois et d'une annee.
Pour cette commande :

```text
debut = 2023-06-01T00:00:00
fin   = 2023-06-02T00:00:00
```

`build_where_clause` produit :

```sql
trip_start_timestamp >= '2023-06-01T00:00:00'
AND trip_start_timestamp < '2023-06-02T00:00:00'
```

Le debut du lendemain est exclu : les deux jours consecutifs ne partagent
pas les memes trajets a minuit. C'est la date de debut du trajet qui compte,
meme si le trajet termine le jour suivant.

## fetch_page : une requete simple

La fonction recoit la source, les options d'ingestion, le filtre et l'offset.
Elle envoie `$where`, `$order`, `$limit`, `$offset` via les parametres structures
de `requests.get`. L'ordre est `trip_start_timestamp, trip_id` pour rendre
la pagination plus stable. Un timeout evite d'attendre indefiniment une requete.

Une reponse HTTP en erreur leve une exception. Une reponse JSON doit etre une
liste de dictionnaires ; une autre forme est une erreur, pas une page vide.
Le contexte `with` ferme la reponse HTTP quand la page est traitee.

## Retry API

La fonction retente uniquement les timeouts et les statuts 429, 500, 502, 503,
504. Un 400 ou 403 echoue immediatement. Une erreur de JSON, de schema de reponse
ou une autre erreur de connexion n'est pas masquee par des tentatives repetitives.

`max_retries` est conserve comme nom de configuration historique ; dans cette
version, il signifie le nombre TOTAL de tentatives, premier appel compris.
Avec 3, une page peut etre demandee au maximum trois fois. Entre deux tentatives,
le code attend `retry_backoff_seconds`, sans jitter ni mecanisme complexe.
Un echec definitif met le run en FAILED. Le log conserve l'exception.

## fetch_all_pages : parcourir les resultats

La premiere page commence a offset 0. Apres une page de 50 000 lignes,
la suivante commence a 50 000. L'offset correspond au nombre de lignes deja
recues. La fonction ajoute chaque page a une liste Python.

Quand une page contient moins de lignes que la limite, y compris zero, la
pagination est terminee. `pagination_complete` devient true. Une page pleine
ne prouve pas qu'il en reste : il faut demander la suivante pour le savoir.

`pages_downloaded` compte les reponses de page reussies, y compris la derniere
page vide. `rows_downloaded` compte les enregistrements, doublons compris.
`max_pages` peut interrompre volontairement la lecture. Si cette limite est
atteinte sur une page pleine, `pagination_complete` reste false.

Le YAML actuel contient `page_size: 50000` et `max_pages: 2`. Le job telecharge
donc au plus 100 000 lignes par execution. Pour enlever cette limite, mettre
`max_pages: null`, apres avoir evalue le volume et la memoire disponibles.
SUCCESS signifie reussite technique du perimetre lu, pas obligatoirement
completude du jour metier.

## records_to_frame : passer de JSON a Spark

La fonction construit la liste des colonnes attendues, puis ajoute les champs
supplementaires presents dans la reponse. Toutes les colonnes Bronze sont
`string`, pour repousser les conversions metier a Silver. Les NULL restent NULL.
Un objet JSON de localisation est conserve comme texte JSON.

Ce choix evite les erreurs d'inference sur une colonne entierement vide et
garantit un schema meme pour une journee sans trajet. Une colonne attendue mais
absente devient NULL : la qualite signale alors le manque. Bronze conserve
les lignes, mais ce n'est pas une archive octet par octet de la reponse HTTP.

## Bronze Data Quality

`bronze_quality` calcule les mesures ci-dessous sur les lignes conservees.
Les anomalies sont des objets `{count, rate}`. Le taux est `count / row_count`,
ou zero quand la table est vide.

| Mesure | Signification |
|---|---|
| row_count | Toutes les lignes, sans suppression |
| column_count | Nombre de colonnes Bronze |
| duplicate_rows | Toutes les lignes de groupes parfaitement identiques |
| duplicate_trip_ids | Toutes les lignes de groupes d'identifiants non vides repetes |
| missing_trip_id | Identifiant NULL ou vide apres trim |
| missing_start_timestamp | Debut manquant |
| missing_end_timestamp | Fin manquante |
| missing_trip_seconds | Duree manquante |
| missing_trip_miles | Distance manquante |
| missing_fare | Tarif manquant |
| missing_trip_total | Total manquant |
| missing_pickup_coordinates | Au moins une coordonnee depart manque |
| missing_dropoff_coordinates | Au moins une coordonnee arrivee manque |

Pour trois lignes dont deux copies identiques, duplicate_rows vaut 2 : on
compte les lignes impliquees, pas le nombre de copies excedentaires.
Un tarif negatif n'est pas une raison de supprimer la ligne a cette etape.
Un texte `ABC` dans une duree n'est pas considere manquant par Bronze ; Silver
essaiera de le convertir. Les deux couches ne mesurent pas la meme chose.

## Profil des colonnes

`column_profile.csv` contient `column`, `dtype`, `null_count`, `null_rate`,
`distinct_count`. Les distincts sont exacts et excluent les NULL. Le profil
compte les NULL SQL ; une chaine vide est donc comptee comme valeur distincte,
alors que les mesures `missing_*` la traitent comme manquante.

Les calculs sont des agregations Spark ; seul le resultat par colonne revient
au driver. Aucun dataset n'est converti par `toPandas()`.

## run_bronze : ordre complet des operations

1. Lire les chemins et creer une partition explicite et un run_id.
2. Creer le logger et le dictionnaire manifest.
3. Ecrire le manifest RUNNING avant les appels API.
4. Appeler fetch_all_pages et enregistrer ses compteurs.
5. Creer le DataFrame Bronze et le mettre en cache pour les calculs repetes.
6. Ecrire le rapport DQ et le profil des colonnes.
7. Ecrire le Parquet dans `_tmp/<run_id>/`.
8. Relire le staging ; verifier le nombre de lignes et le schema.
9. Ajouter `_bronze_run.json`, puis promouvoir le dossier entier.
10. Passer en SUCCESS ; en cas d'exception, passer en FAILED et la relancer.
11. Dans finally : terminer le manifest, liberer le cache, nettoyer le staging
    restant et fermer le logger.

`main` lit les deux arguments CLI, charge le YAML, cree Spark, appelle le job
et arrete Spark dans un `finally`. Le job recoit une session pour permettre
les petits tests locaux ; ce n'est pas un framework d'injection.

## Manifest et logs

Le manifest contient run_id, processing_date, status, started_at, finished_at,
source, rows_downloaded, pages_downloaded, pagination_complete, output_path,
column_count et error_message. started_at est l'heure technique UTC du run,
pas la date des trajets. finished_at reste NULL pendant RUNNING.

Un fichier log porte le nom du run_id. Il montre le demarrage, chaque page,
le total de lignes, l'ecriture, la validation et le resultat. `logger.exception`
ajoute la pile d'erreur pour comprendre un FAILED.

Exemple illustratif, pas resultat d'un appel API reel :

```json
{
  "run_id": "BRZ_20260913T120000Z_a1b2c3d4",
  "processing_date": "2023-06-01",
  "status": "SUCCESS",
  "rows_downloaded": 3,
  "pages_downloaded": 1,
  "pagination_complete": true
}
```

## Ou sont les sorties ?

```text
data/bronze/chicago_taxi/processing_date=2023-06-01/part-*.parquet
data/bronze/chicago_taxi/processing_date=2023-06-01/_bronze_run.json
data/reports/bronze/<run_id>/manifest.json
data/reports/bronze/<run_id>/bronze_dq_report.json
data/reports/bronze/<run_id>/column_profile.csv
data/logs/<run_id>.log
```

Les racines sont configurables dans le YAML. Le dossier `data/logs` est la
valeur locale choisie, plutot qu'un deuxieme dossier `logs` a la racine.
Un rerun de la meme date remplace les donnees apres validation ; ses rapports
restent distincts. Lire le statut avant d'utiliser une sortie.

## Limites et tests

Les pages sont accumulees dans une liste Python : cette ingestion n'est pas
une extraction distribuee de plusieurs dizaines de Go. Une source modifiee
pendant une pagination par offset peut changer les resultats ; l'ordre stable
ne fournit pas un snapshot distant. Les champs inattendus sont conserves,
mais aucun contrat ne rejette automatiquement leur apparition.

Les tests simulent l'API : date, filtre, pages, limite, retries, qualite,
manifest RUNNING/SUCCESS et Parquet. Ils ne telechargent pas de donnees reelles.
Apres ces verifications, Silver peut etre teste sur les partitions fixture.
Le code complet est dans l'annexe [code_pipeline_simple.md](reports/code_pipeline_simple.md).
