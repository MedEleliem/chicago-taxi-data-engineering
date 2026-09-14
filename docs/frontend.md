# Frontend HTML, CSS et JavaScript

Les fichiers actifs sont `frontend/index.html`, `frontend/css/style.css` et
`frontend/js/app.js`. FastAPI sert l'HTML sur `/` et les ressources sur `/static`.
Il n'y a pas de compilation, React, Node ou serveur frontend distinct.

## Les quatre vues

Overview affiche six KPI, les volumes quotidiens, les revenus quotidiens et
la distribution horaire. Operations affiche duree, distance, taxis distincts,
une heatmap jour de semaine/heure, les classements et une table de trajets.
Geography propose densite, trips par zone, revenu par zone et Trip Explorer.
Data Quality affiche les volumes, taux, funnel, regles et identifiants de runs.

La navigation utilise le hash de l'URL et masque les sections inactives.
Le bouton retour du navigateur fonctionne donc avec les quatre vues.
Les filtres sont envoyes a l'API, jamais appliques sur un gros fichier brut
dans le navigateur. La qualite supporte seulement les dates : les deux autres
selecteurs sont desactives dans cette vue.

## Fonctions JS a lire dans l'ordre

`boot` charge health, les dates et les options. `navigate` choisit la section
et lance `reload`. `selectedParams` construit les filtres. `api` centralise
fetch et les erreurs HTTP. Les fonctions fetchKPIs, fetchDaily, fetchHourly,
fetchZones, fetchTrips et fetchDQ utilisent ce client.

`reload` charge uniquement les donnees utiles a la page. Un compteur de requete
empeche un chargement ancien d'ecraser une selection plus recente. `drawOverview`,
`drawOperations`, `drawQuality` et `drawTrips` rendent leurs sections. Les valeurs
textuelles issues des donnees sont echappees avant insertion HTML.

Les graphes utilisent Plotly.react, des dimensions stables, des axes sobres,
des info-bulles et le redimensionnement automatique. Les KPI utilisent Intl
via toLocaleString. Les moyennes absentes ne sont pas presentees comme zero.
Une entree sans donnees affiche un message explicite.

La table demande douze lignes par page. Le serveur gere recherche, tri et
pagination. Les noms de colonnes triables sont limites, pas interpretes comme
du code. Cliquer un trip_id ouvre le detail dans Geography.

## Carte et CARTO

MapLibre dessine des couches WebGL, pas des milliers de marqueurs DOM.
`ensureMap` initialise la carte une fois. `drawMap` actualise les sources
pickups, routes et areas. `updateMapMode` change les visibilites et la taille
des cercles. Les valeurs par zone sont placees au centre moyen des pickups
disponibles : ce sont des symboles proportionnels, pas des polygones administratifs.

Le fond CARTO Voyager utilise la cle fournie dans CARTO_BASEMAP_KEY. Le navigateur
lit `/api/map-style` et demande `/api/tiles/z/x/y.png`. Le backend ajoute la cle
uniquement a l'appel CARTO, garde un cache memoire borne de 256 tuiles et envoie
un en-tete de cache navigateur. La cle n'est pas dans le JavaScript ni dans le
JSON du style. Les attributions CARTO et OpenStreetMap restent visibles.
Sans cle CARTO, le style utilise les tuiles OpenStreetMap.

La carte doit disposer d'Internet pour les tuiles. Les erreurs affichent un
message sans retirer les donnees de trajet. Une ligne est une liaison droite
entre centroides publies, jamais un chemin reel ; le texte correspondant est
visible sous la carte. Le GeoJSON est borne et annonce le nombre affiche et
le nombre total eligible.

`showTrip` construit le panneau lateral avec horaires, compagnie, paiement,
montants, duree, distance et zones. Le bouton de lieux proches est le seul
declencheur Foursquare. Aucun appel automatique par trajet n'est fait.
Une absence de cle ou un timeout affiche une indisponibilite et laisse le
reste de l'application utilisable.

## Design et accessibilite

Les couleurs, bordures et espacements partages sont des variables CSS. Les
sections analytiques sont ouvertes, les cartes servent aux KPI individuels.
La sidebar devient une navigation inferieure sur petit ecran. Les tableaux
ont un defilement horizontal, la carte et son panneau passent sur deux lignes.
Les tailles de police ne dependent pas de la largeur du viewport.

Les champs ont des labels, les icones ont title/aria-label, la navigation a
un etat actif, le focus clavier est visible et un lien Skip to content existe.
Les animations sont limitees au chargement et aux transitions courtes ;
prefers-reduced-motion les desactive. Les icones viennent de Lucide.

## Dependances et limites

Plotly, MapLibre, Lucide et Inter sont charges depuis leurs CDN dans cette
version sans build. Une connexion est necessaire au premier chargement. Les
versions JS sont explicites dans index.html. Pour un environnement sans reseau,
il faudra distribuer ces bibliotheques et un fond de carte local autorise.

Les fonctions de serving agre gent la projection Gold cote serveur. Les
graphiques ne deviennent pas une deuxieme implementation de Silver. Le jeu
de demonstration est separe sous data/demo et signale par une banniere.

References des bibliotheques :
[MapLibre](https://maplibre.org/maplibre-gl-js/docs/examples/display-a-map/),
[fichiers statiques FastAPI](https://fastapi.tiangolo.com/tutorial/static-files/).
