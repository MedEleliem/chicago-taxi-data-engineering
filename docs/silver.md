# Silver PySpark, du typage a la quarantine

## Objectif

Silver lit une seule partition Bronze, applique la logique metier du notebook
[pandas_only_draft.executed.ipynb](../notebooks/pandas_only_draft.executed.ipynb),
puis ecrit les lignes valides et les lignes rejetees dans deux sorties Parquet.
Un warning seul ne retire jamais une ligne de Silver Valid.

Tout le code actif se trouve dans [src/silver/__main__.py](../src/silver/__main__.py).
Le schema est un dictionnaire, les regles sont des expressions Spark et le
manifest est un dictionnaire JSON. La sequence peut se lire directement dans
`run_silver`, sans parcourir des classes ni un framework DataOps.

## Comment lancer

Depuis la racine, apres une execution Bronze reussie pour la meme date :

```powershell
python -m src.bronze --processing-date 2023-06-01
python -m src.silver --processing-date 2023-06-01
```

Pour un autre fichier de configuration :

```powershell
python -m src.silver --processing-date 2023-06-01 --config config/chicago_taxi.yml
```

Silver ne contacte pas l'API. `read_bronze` construit le chemin exact
`data/bronze/chicago_taxi/processing_date=2023-06-01/`. Une partition inexistante
ou sans Parquet est une erreur. Une partition Parquet contenant zero ligne
est en revanche une entree vide valide techniquement.

## Le schema

`SCHEMA` associe chaque nom a son type Spark. Les identifiants et les codes
census restent des textes. Les montants sont des `double`, comme demande
pour cette version simple.

| Colonnes | Type |
|---|---|
| trip_id, taxi_id | string |
| trip_start_timestamp, trip_end_timestamp | timestamp |
| trip_seconds | long |
| trip_miles | double |
| pickup_census_tract, dropoff_census_tract | string |
| pickup_community_area, dropoff_community_area | int |
| fare, tips, tolls, extras, trip_total | double |
| payment_type, company | string |
| pickup_centroid_latitude, pickup_centroid_longitude | double |
| dropoff_centroid_latitude, dropoff_centroid_longitude | double |
| pickup_centroid_location, dropoff_centroid_location | string |

Les deux colonnes de localisation textuelle sont conservees pour rester
compatibles avec le notebook. Les nombres `double` sont des flottants binaires :
ils ne representent pas tous les centimes exactement. Une version comptable
plus stricte pourrait choisir `DecimalType`, avec une precision et une politique
d'arrondi explicites. Cette version ne le fait pas.

## cast_columns, ligne par ligne dans la logique

La fonction commence par normaliser les noms : espaces de bord retires,
minuscules et suites d'espaces remplacees par `_`. Par exemple,
`Trip Start Timestamp` devient `trip_start_timestamp`.
Si deux noms deviennent identiques, elle echoue pour eviter une ambiguite.

Pour chaque nom du schema, une colonne absente est ajoutee avec NULL. Puis :

- les timestamps utilisent `try_to_timestamp`, avec conversion generale et
  un format `MM/dd/yyyy hh:mm:ss a` de secours ;
- les autres valeurs utilisent `try_cast`, qui retourne NULL en cas de valeur
  non convertible, meme avec le mode ANSI Spark ;
- un resultat numerique NaN en double est transforme en NULL.

La boucle parcourt les noms de colonnes. Elle construit un plan Spark, elle
ne convertit pas chaque ligne dans une boucle Python. Les colonnes non prevues
par le contrat ne sont pas recopiees vers Silver, mais restent dans Bronze.

Exemples : `"600"` devient l'entier 600, `"2.5"` devient 2.5 et `"ABC"`
dans trip_seconds devient NULL. Une date mal formee devient NULL et declenche
une erreur de timestamp. Une duree NULL ne declenche pas INVALID_DURATION :
cette nuance vient de la logique `notna() & <= 0` du notebook et est conservee.

## Les colonnes derivees

`add_derived_columns` ajoute les champs utiles suivants, avec des fonctions Spark.

| Colonne | Calcul | Exemple |
|---|---|---|
| trip_date | Date du debut | 2023-06-01 |
| trip_year | Annee du debut | 2023 |
| trip_month | Mois du debut | 6 |
| trip_hour | Heure du debut | 10 |
| calculated_trip_seconds | Timestamp fin moins debut, en secondes | 600 |
| trip_duration_minutes | trip_seconds / 60 | 10 |
| has_tip | tips > 0, NULL donne false | true pour tips=2 |
| tip_rate | tips / fare quand fare > 0 | 0.2 pour 2 / 10 |

La duree calculee conserve les fractions de seconde. `tip_rate=0.2` signifie
20 %. Si fare vaut zero, une valeur negative ou NULL, tip_rate reste NULL
pour eviter une division invalide. Les dates source sans fuseau sont interpretees
dans la convention UTC de session ; le job n'ajoute pas une conversion horaire
Chicago et ne corrige pas l'arrondi des heures de la source.

## apply_quality_rules

Cette fonction marque d'abord les doublons puis prepare deux dictionnaires :
nom de regle -> condition Spark. `rule_names` transforme une condition vraie
en son nom et retire les NULL du tableau obtenu.

Par exemple :

```python
errors = {
    "INVALID_DURATION": F.col("trip_seconds") <= 0,
    "NEGATIVE_DISTANCE": F.col("trip_miles") < 0,
}
```

Pour un trajet de duree 0 et de distance -1, le resultat comprend :

```text
quality_errors = ["INVALID_DURATION", "NEGATIVE_DISTANCE"]
has_error = true
```

Une comparaison avec NULL n'est pas vraie en SQL. Elle n'ajoute donc aucune
regle au tableau. La lambda utilisee par `F.filter` construit une expression
native Spark : il ne s'agit pas d'une Python UDF executant du code par ligne.

## ERROR : liste complete

| Regle | Quand elle s'applique |
|---|---|
| MISSING_TRIP_ID | trip_id NULL ou chaine vide apres trim |
| INVALID_START_TIMESTAMP | Debut NULL apres casting |
| INVALID_END_TIMESTAMP | Fin NULL apres casting |
| END_BEFORE_START | Fin anterieure au debut |
| INVALID_DURATION | trip_seconds <= 0 |
| NEGATIVE_DISTANCE | trip_miles < 0 |
| NEGATIVE_FARE | fare < 0 |
| NEGATIVE_TIPS | tips < 0 |
| NEGATIVE_TOLLS | tolls < 0 |
| NEGATIVE_EXTRAS | extras < 0 |
| NEGATIVE_TRIP_TOTAL | trip_total < 0 |

Une erreur suffit a envoyer la ligne en quarantine. Toutes les erreurs
applicables sont conservees : le traitement ne s'arrete pas a la premiere.
Le controle d'identifiant vide et les controles de bornes geographiques sont
des extensions explicites par rapport a la fonction Silver du notebook.

## WARNING : liste complete

| Regle | Quand elle s'applique |
|---|---|
| ZERO_DISTANCE | trip_miles vaut exactement 0 |
| DURATION_MISMATCH | Ecart absolu entre trip_seconds et calculated_trip_seconds > 60 |
| PARTIAL_PICKUP_COORDINATES | Une seule coordonnee depart est NULL |
| PARTIAL_DROPOFF_COORDINATES | Une seule coordonnee arrivee est NULL |
| INVALID_PICKUP_LATITUDE | Latitude depart hors [-90, 90] |
| INVALID_PICKUP_LONGITUDE | Longitude depart hors [-180, 180] |
| INVALID_DROPOFF_LATITUDE | Latitude arrivee hors [-90, 90] |
| INVALID_DROPOFF_LONGITUDE | Longitude arrivee hors [-180, 180] |
| DUPLICATED_TRIP_ID | trip_id appartient a un groupe de plusieurs lignes |

Un ecart de 60 secondes exactement n'est pas un warning ; 61 secondes en est
un. Le seuil vient du notebook. Deux coordonnees absentes ne forment pas une
paire partielle. Les bornes geographiques sont des limites physiques, pas des
seuils statistiques inventes. Aucun seuil de tarif, distance ou duree maximale
n'est ajoute dans cette version.

## Doublons transparents

Deux fenetres `Window.partitionBy` comptent les lignes d'un meme groupe.
`is_exact_duplicate` compare les colonnes canoniques et derivees avant lineage.
`is_trip_id_duplicate` compare seulement l'identifiant.

Si deux lignes ont la meme cle mais des distances differentes, elles sont des
doublons de cle sans etre exactement identiques. Toutes les lignes du groupe
sont marquees, comme `duplicated(keep=False)` en Pandas. Les identifiants NULL
repetes sont aussi marques ; ils ont deja une erreur MISSING_TRIP_ID.
Aucune ligne n'est dedupliquee silencieusement.

La notion d'exactitude concerne ici le contrat canonique apres conversion.
Des colonnes Bronze supplementaires ignorees ou des textes convertis de la
meme facon ne distinguent plus deux lignes en Silver.

## split_valid_and_quarantine

La fonction ne fait que deux filtres complementaires :

```python
valid = df.filter(~F.col("has_error"))
quarantine = df.filter(F.col("has_error"))
```

| Erreur | Warning | Sortie |
|---|---|---|
| Non | Non | Silver Valid |
| Non | Oui | Silver Valid |
| Oui | Non | Quarantine |
| Oui | Oui | Quarantine |

Quarantine conserve les regles, les valeurs converties et les colonnes de
provenance. Pour retrouver le texte brut d'un nombre non convertible, revenir
a Bronze. Le job ne stocke pas une copie brute supplementaire par ligne rejetee.

## quality_report et reconciliation

Le JSON dq_report contient input_rows, valid_rows, rejected_rows, warning_rows,
rejection_rate, warning_rate, duplicate_trip_rows et exact_duplicate_rows.
warning_rows compte les lignes avec warning dans TOUTE l'entree, y compris
les lignes deja rejetees. Ce n'est pas le nombre de warnings du seul Silver Valid.

Exemple : une ligne correcte, une ligne avec distance zero et une ligne avec
duree zero plus ecart de duree donnent 3 entrees, 2 valides, 1 rejetee et 2
lignes avec warning. Les taux sont 1/3 de rejet et 2/3 de warning.

`validate_reconciliation` impose :

```text
input_rows = valid_rows + rejected_rows
warning_rows <= input_rows
```

Si un controle echoue, une exception rend le job FAILED. Pour une entree vide,
tous les comptes et taux valent zero, et les deux datasets Parquet vides sont
ecrits avec leur schema. Cela ne decide pas si une journee vide est acceptable
pour le metier : cette politique appartient a une future etape.

## rule_report et rapports CSV

La fonction explose le tableau d'erreurs ou warnings, groupe par nom et compte
les occurrences. Chaque ligne contient `rule`, `count`, `rate`, avec comme
denominateur le nombre total de trajets d'entree.

Une ligne avec trois erreurs contribue trois fois aux comptes de regles, mais
une seule fois a rejected_rows. La somme des comptes d'erreurs peut donc depasser
le nombre de lignes. Les CSV listent seulement les regles observees et conservent
un en-tete quand aucune regle n'est observee.

Le profil supplementaire `column_profile.csv` decrit toutes les colonnes du
DataFrame apres qualite et lineage, avant separation. Il contient les types,
NULL et distincts exacts. Les NULL sont exclus des distincts. Les petits resultats
agreges sont rapatries au driver ; les trajets restent traites par Spark.

## find_source_run et lineage

La fonction lit en priorite `_bronze_run.json` dans la partition. Ce marqueur
a ete ecrit par Bronze et deplace avec son Parquet. S'il manque, la fonction
cherche le dernier manifest Bronze SUCCESS de meme date et chemin absolu.
Si aucune provenance n'est disponible, le job echoue explicitement.

Le manifest Silver porte `source_run_id`. Chaque ligne porte aussi :

```text
_run_id                execution Silver
_source_run_id         execution Bronze
_processing_date       jour metier de la partition
_processing_timestamp  heure technique de debut Silver
```

Un timestamp technique constant est ajoute avec `F.lit` pour toutes les lignes
du run. Ces metadonnees changent lors d'un rerun, contrairement au contenu
metier attendu pour une entree identique. La provenance n'est pas un checksum.

## run_silver : ordre de lecture du code

1. Construire les trois chemins de partition et verifier qu'ils sont distincts.
2. Generer le run_id, les dossiers de rapports/staging, le logger et le manifest.
3. Ecrire RUNNING, lire Bronze, puis retrouver source_run_id.
4. Creer successivement `typed`, `derived`, `checked` : typage, derives, regles.
5. Ajouter le lineage et mettre ce seul DataFrame en cache.
6. Separer valid et quarantine, calculer et reconcilier les comptes.
7. Ecrire les rapports DQ, erreurs, warnings et profil.
8. Ecrire et relire les deux stagings pour verifier nombre de lignes et types.
9. Promouvoir Silver, puis quarantine, et passer en SUCCESS.
10. En cas d'exception : FAILED, message d'erreur et trace dans le log.
11. Dans finally : ecrire le manifest termine, liberer le cache, nettoyer les
    stagings restants et fermer le logger.

La fonction `main` cree et arrete Spark autour de ce traitement. Le manifest
contient les dates techniques, les chemins, les compteurs, les taux et le statut.
Il est ecrit via un fichier temporaire puis remplace. Un taux de rejet eleve
ne rend pas le job FAILED a lui seul : Silver calcule la qualite, sans Quality Gate.

## Outputs

```text
data/silver/chicago_taxi/processing_date=2023-06-01/part-*.parquet
data/quarantine/chicago_taxi/processing_date=2023-06-01/part-*.parquet
data/reports/silver/<run_id>/manifest.json
data/reports/silver/<run_id>/dq_report.json
data/reports/silver/<run_id>/errors_report.csv
data/reports/silver/<run_id>/warnings_report.csv
data/reports/silver/<run_id>/column_profile.csv
data/logs/<run_id>.log
```

Le dossier `_tmp/<run_id>` est temporaire ; un dossier parent `_tmp` vide peut
rester sans poser de probleme. Une relance ne concatene pas anciens et nouveaux
Parquets. Les rapports et logs ont un nouveau run_id et restent consultables.

## Deduplication de la version livree

Le job actif appelle maintenant deduplicate_trips apres les regles qualite.
Il conserve au plus une ligne valide par trip_id. Il privilegie une ligne
sans erreur, puis un ordre lexical stable de la representation JSON des valeurs
metier. Les copies ecartees restent en quarantaine avec l'erreur
DUPLICATE_TRIP_ID_EXCLUDED. Les doublons strictement identiques sont equivalents
pour les KPI. Le warning DUPLICATED_TRIP_ID reste visible sur toutes les occurrences.
Les paragraphes precedents decrivant uniquement la detection correspondent a
la premiere migration fidele au notebook, avant cette evolution de livraison.

## Limites a comprendre

- Les numeriques NULL restent permis selon la V1. Il n'existe pas encore une
  erreur par valeur numerique absente ou mal formee.
- Les doubles ne donnent pas une precision comptable exacte ; DecimalType est
  une evolution possible, pas une fonctionnalite active.
- Les valeurs infinies ne disposent pas d'une regle generale specifique.
- Les deux promotions ne sont pas atomiques ensemble ; attendre SUCCESS et
  relancer apres un echec partiel. Pas de jobs concurrents sur la meme date.
- En mode filesystem seul, les partitions remplacees ne sont pas archivees.
  En mode Compose, les publications S3 sont conservees par run_id.
- Le fallback de lineage se base sur les manifests ; un manifest corrompu peut
  faire echouer sa lecture. Reproduire Bronze est le parcours normal.
- run_stage exige la completude de Bronze via DataOps avant publication S3.
- Les distincts exacts et fenetres de doublons sont a mesurer sur un volume reel.

## Tests et code complet

`tests/test_silver.py` couvre le schema double, derives, erreurs, warnings,
split, doublons, NULL, reconciliation, validation de staging, ecriture avec
lineage, rerun, entree vide et partition absente. Il utilise une petite source
Bronze simulee, pas un telechargement reseau. Les details d'execution sont dans
[verification_resultats.md](reports/verification_resultats.md).
L'annexe [code_pipeline_simple.md](reports/code_pipeline_simple.md) contient
tous les fichiers actifs et les deux fichiers de tests dans leur integralite.
