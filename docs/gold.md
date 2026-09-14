# Gold : des trajets fiables aux indicateurs

Reference : la cellule Gold de `notebooks/pandas_only_draft.executed.ipynb`.
Point d'entree actif : `src/gold/__main__.py`. Les anciens modules Gold restent
conserves ; ils ne sont pas importes par cette commande.

```powershell
python -m src.gold --processing-date 2023-06-01
```

## Responsabilite

Gold lit une partition Silver Trusted et son manifest SUCCESS. Il refuse des
lignes avec has_error=true ou NULL. Il ne corrige pas les types, ne nettoie
pas les trajets et ne produit aucune quarantine. Il selectionne les colonnes
necessaires a l'analyse et remplace seulement les libelles company/payment_type
NULL par UNKNOWN, comme dimension analytique.

## Tables et grains

| Dossier | Une ligne represente |
|---|---|
| daily | Une date de trajet |
| hourly | Une date et une heure |
| zones | Une zone de pickup, y compris le groupe NULL |
| payments | Un moyen de paiement |
| companies | Une compagnie |
| kpi_summary | Le bilan de toute la partition |
| trips | Un trajet valide, projection analytique limitee |
| geo | Un trajet dont les quatre coordonnees sont utilisables |

Toutes ces tables vivent sous `data/gold/chicago_taxi/processing_date=YYYY-MM-DD/`.
Les tables de dimensions sont donc calculees pour une partition de traitement.
Une API qui combine plusieurs jours doit agreger les valeurs, pas faire la
moyenne non ponderee des moyennes quotidiennes.

`aggregate_metrics` compte les lignes, additionne trip_total pour le revenu,
calcule les moyennes de montant/distance/duree, totalise tips et compte has_tip.
Le taux de trajets avec pourboire est tipped_trip_count / trip_count. Le revenu
utilise trip_total, pas fare seul. Les moyennes Spark excluent les NULL. Les
sommes sans valeur donnent zero, pour correspondre aux sommes Pandas.
daily expose avg_revenue_per_trip ; les autres tables utilisent avg_trip_total.
Quelques colonnes additives supplementaires sont conservees pour la lecture.

`kpi_summary` ajoute les taxis et compagnies distincts non NULL. Une compagnie
inconnue ne compte pas comme une compagnie reelle dans cet indicateur.
Sur une entree vide, total_trips et les sommes valent zero ; les moyennes sont NULL.

## Geo et projection trips

La projection trips permet les filtres et le detail dans l'application sans
exposer les colonnes techniques ou les tableaux de qualite Silver. Elle contient
les champs de trajet demandes, plus trip_date, trip_hour et has_tip utiles au
serving. Elle garde aussi les trajets sans localisation pour ne pas fausser
les KPI. Ce n'est pas une copie integrale de Silver.

geo exige les deux latitudes dans [-90,90] et les deux longitudes dans [-180,180].
Les NULL et NaN ne passent pas ces conditions. La reduction geographique ne
change jamais les indicateurs globaux. Une liaison pickup/dropoff represente
des centro ides publies, pas un itineraire GPS ou routier.

## Validation et publication

`validate_tables` compare les sommes trip_count de daily, hourly, zones,
payments et companies au nombre de lignes Silver ; kpi_summary est aussi
controle. Le resultat est conserve dans gold_validation_report.json.
Une reconciliation critique fausse provoque FAILED avant toute publication.

Les tables sont ecrites dans un dossier temporaire commun, puis chacune est
relue pour controler schema et nombre de lignes. Le dossier Gold entier est
promu en une seule operation locale. Un _manifest.json et un _validation.json
voyagent avec les tables. Les rapports de run et logs restent dans les racines
habituelles. L'API ne doit lire que des publications marquees SUCCESS.

## Lineage

La partition Silver doit porter un unique _run_id. Le manifest correspondant
doit etre SUCCESS et decrire le meme chemin et la meme date. Une partition vide
utilise le dernier manifest correspondant. Gold conserve source_run_id=SLV_RUN
et bronze_run_id=BRZ_RUN. Les empreintes du contenu ne sont pas calculees.

## Tests et limites

`tests/test_gold.py` teste revenu, taux, UNKNOWN, geo, refus des erreurs,
reconciliation et maintien de l'ancienne publication en cas de validation
critique echouee. Les tests sont locaux et utilisent une petite fixture.
Les montants double gardent la precision de Silver ; aucun DecimalType n'est
introduit. Les moyennes et sommes peuvent subir l'arrondi flottant normal.
La projection trips peut etre volumineuse ; le serving doit rester borne
et le navigateur ne doit jamais recevoir tout le dataset.
