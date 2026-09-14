# DataOps : controles, Quality Gate et audit

Le module actif est `src/dataops/__main__.py`. Il ne transforme pas les trajets
et ne demarre pas Spark. PyArrow lit les metadonnees Parquet et les identifiants
de provenance Silver ; les regles metier restent exclusivement dans Silver.

## Ordre manuel

Apres Bronze, lancer `python -m src.dataops --layer bronze --processing-date 2023-06-01`.
Apres Silver, lancer la meme commande avec `--layer silver`.
Ne lancer Gold que si le controle Silver termine avec le code de sortie zero.
Apres Gold, lancer la meme commande avec `--layer gold`.
Pour verifier la demonstration, ajouter `--reports-root data/demo/reports`.

## Fonctions, dans l'ordre de lecture

1. `quality_gate` verifie les seuils, les compteurs, la reconciliation et le taux
   de rejet. Une incoherence provoque une exception, jamais un PASS silencieux.
2. `latest_manifest` selectionne la tentative la plus recente de la date,
   y compris un echec : une ancienne reussite ne masque pas une relance ratee.
3. `audit_run` normalise les manifests des trois couches dans un modele commun.
   Pour Gold, output_rows compte les trajets, pas la somme des tables agregees.
4. `parquet_rows` compte les lignes des fichiers effectivement publies et peut
   verifier que toutes les valeurs `_run_id` correspondent au run attendu.
5. `validate_publication` compare les donnees au manifest. Bronze exige par
   defaut une pagination complete ; Silver controle Trusted et Quarantine ;
   Gold controle les huit tables et le marqueur de validation.
6. `run_check` combine ces controles, verifie le lien au dernier run source,
   puis enregistre l'audit. Une exception produit aussi un audit FAIL.
7. `main` expose la commande ; FAIL retourne le code 1, PASS/WARNING le code 0.

## Politique configurable

Dans `config/chicago_taxi.yml`, `dataops.warning_threshold=0.02` et
`failure_threshold=0.05` sont des choix initiaux du projet, pas des seuils
officiels Chicago. PASS : moins de 2 % de rejets ; WARNING : de 2 % inclus
a 5 % exclus ; FAIL : au moins 5 %. Les seuils peuvent etre ajustes dans YAML.
Les avertissements metier Silver ne sont pas des rejets et ne bloquent pas
le gate. `allow_empty=false` bloque un lot vide. `require_complete_bronze=true`
bloque une extraction tronquee par max_pages, meme si Bronze a termine.

## Validation humaine

Les contrôles automatiques vérifient ce qui peut être exprimé de manière
déterministe. Le DAG ajoute ensuite
`approve_data_and_business_quality`, basé sur l'`ApprovalOperator` natif
d'Airflow. Le reviewer confirme la réconciliation, le taux de rejet et la
cohérence métier avant la construction de Gold. **Approve** continue le DAG;
**Reject** arrête les tâches suivantes. Cette décision ne remplace jamais un
Quality Gate automatique en échec.

## Audit et limites

`data/reports/audit/<run_id>.json` contient run_id, processing_date, layer,
source_run_id, started_at, finished_at, status, input_rows et output_rows.
check_status distingue la reussite technique du job du resultat DataOps.
Une nouvelle verification du meme run remplace son audit ; les manifests
historiques des runs restent conserves. Ce n'est pas un journal inviolable.

Les commandes standalone Gold peuvent contourner le gate : respecter l'ordre
documente ou utiliser l'orchestrateur. Pas de verrou distribue ; ne pas lancer
deux pipelines simultanes sur la meme date. Une verification ne fournit pas
une transaction entre plusieurs processus. Les chemins des manifests sont
locaux : pour deplacer des donnees, regenerer les publications dans la nouvelle
racine. Aucun nettoyage ni suppression des anciens runs n'est automatique.
