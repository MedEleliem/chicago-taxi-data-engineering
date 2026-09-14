# Graphify : l'ensemble du projet

La restriction au seul notebook est annulee. Le graphe principal est maintenant
`graphify-out/graph.json` et sa vue interactive `graphify-out/graph.html`.
L'ancien graphe sous graphify-out/graphify-out reste un artefact historique,
pas le point d'entree actuel.

## Ouvrir et naviguer

Ouvrir graphify-out/graph.html dans le navigateur : aucun serveur necessaire.
Utiliser la recherche du graphe avec run_bronze, run_silver, run_gold,
quality_gate, service.py ou app.js. Les anciens modules apparaissent aussi :
les points d'entree actifs du pipeline sont les __main__.py, pas les anciens job.py.

Depuis la racine du projet, dans PowerShell :

```powershell
& "$env:USERPROFILE\.local\bin\graphify.exe" query "quality_gate run_gold"
& "$env:USERPROFILE\.local\bin\graphify.exe" explain "run_silver"
& "$env:USERPROFILE\.local\bin\graphify.exe" affected "write_staging"
& "$env:USERPROFILE\.local\bin\graphify.exe" update .
& "$env:USERPROFILE\.local\bin\graphify.exe" export html --graph graphify-out/graph.json
```

## Perimetre et fidelite

src, frontend, tests, scripts, dags, config et documentation font partie du
perimetre. .graphifyignore exclut les fichiers .env, donnees, dependances,
rapports generes, caches et copies, dont chicago_taxi_app pour ne pas doubler
chaque fonction. Le dossier autonome peut avoir son propre graphe en lancant
la commande update depuis sa racine.

L'extraction locale du code utilise les structures et relations reconnues par
Graphify, sans cle API. La presence de titres et liens documentaires ne signifie
pas une analyse semantique exhaustive de chaque paragraphe. Les notebooks
.ipynb, diagrammes .dot et autres formats non reconnus sont signales comme
non classes ; ils ne sont pas silencieusement presentes comme analyses.
Le JSON et GRAPH_REPORT.md donnent les fichiers sources, niveaux de confiance
et limites. Les tests executables restent la preuve du comportement reel.

La memoire de l'ancienne consigne notebook-only a ete corrigee, afin que les
prochaines sessions utilisent le nouveau perimetre.
