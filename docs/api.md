# API locale

Deux fichiers : `src/api/main.py` declare les routes et valide les parametres ;
`src/api/service.py` lit les publications Gold et construit les reponses.
Aucune SparkSession n'est demarree par l'API.

```powershell
python -m pip install -r requirements-app.txt
python -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

Swagger : http://localhost:8000/docs. Health : http://localhost:8000/api/health.
La racine de stockage est `data/`, modifiable par CHICAGO_DATA_ROOT. L'API ne
sert ni la racine data, ni les cles, ni les Parquets comme fichiers statiques.

## Routes

| GET | Reponse |
|---|---|
| /api/health | Etat, disponibilite Gold, dates, options et mode demo/live |
| /api/kpis | KPI globaux pour la selection |
| /api/daily, /api/hourly | Agregats temporels |
| /api/zones, /api/payments, /api/companies | Agregats par dimension |
| /api/trips | items, total, offset, limit |
| /api/trips/{trip_id} | Premier trajet correspondant et nombre de correspondances |
| /api/geo/pickups, /api/geo/trips | GeoJSON borne, total eligible et nombre retourne |
| /api/data-quality/summary | Volumes, taux et lineage des runs publies |
| /api/data-quality/errors, /api/data-quality/warnings | Comptes de regles et taux ponderes |
| /api/places/nearby | Jusqu'a cinq lieux, ou indisponibilite non bloquante |

## Filtres et precision des calculs

start_date/end_date sont inclusifs ; company, payment_type et pickup_area sont
des egalites. Un intervalle inverse donne 422. La table trips accepte limit
de 1 a 1000, offset, search litteral et un tri parmi quatre colonnes connues.
Le GeoJSON est limite a 5000 features, avec echantillonnage deterministe.
Les reponses JSON convertissent NaN et les valeurs absentes en null.

La projection Gold trips est mise en cache Pandas et invalidee lorsque les
run_id publies changent. Les agregats filtres sont recalcules cote serveur
sur cette projection Gold, jamais sur Silver. Cela garde des moyennes exactes
pour la selection, meme avec des NULL et des jours de volumes differents.
C'est un compromis local : la projection trips n'est pas une petite table
agregee et doit tenir en RAM. Pour des millions de trajets, preferer Arrow/DuckDB
avec filtres pousses au stockage ou des cubes Gold dedies. Le navigateur ne
recoit que des agregats et des listes bornees.

## Qualite et lineage

Les rapports DQ sont ceux des runs Silver et Bronze references par chaque
publication Gold. Ils ne sont pas choisis au hasard parmi les derniers fichiers.
Les taux combines divisent les comptes totaux par le volume total ; ils ne
moyennent pas les taux par jour. Les rapports supportent uniquement les dates.
Un filtre company/payment/pickup_area sur DQ donne 422 plutot que d'etre ignore.
Une publication ou un rapport manquant donne 503 ; health reste accessible.

## Foursquare optionnel

FOURSQUARE_API_KEY reste cote serveur. L'appel est a la demande, timeout cinq
secondes, rayon 500 m, limite cinq resultats. Le cache local
`data/cache/foursquare.json` utilise les coordonnees arrondies a quatre decimales.
Une absence de cle ou une erreur du service retourne available=false sans
faire tomber l'application. Le cache est sans expiration dans cette version.
Un verrou local evite les collisions dans un processus ; utiliser un seul worker.

Endpoint et authentification verifies dans la
[documentation officielle Foursquare](https://docs.foursquare.com/fsq-developers-places/reference/authentication).
La cle de service utilise Bearer et X-Places-Api-Version=2025-06-17.
L'enrichissement n'a pas ete teste avec une cle reelle.

## Tests et limites

`tests/test_api.py` utilise TestClient et des Parquets fixture. Il teste filtres,
bornes, detail, GeoJSON, entree absente, JSON vide, DQ/lineage et Foursquare absent
ou en timeout. Pas d'authentification applicative : l'API est destinee au poste
local. Ne pas l'exposer directement sur Internet. Les lectures concurrentes
pendant une publication locale ne sont pas des transactions de base de donnees.
