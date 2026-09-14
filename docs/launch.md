# Lancer les jobs et les serveurs manuellement

Toutes les commandes se lancent depuis la racine de l'application, celle qui
contient src, config et frontend. Dans le dossier isole, cette racine est
`chicago_taxi_app`. Les terminaux ci-dessous sont des fenetres PowerShell.

Pour valider le test avec stockage objet et orchestration, utiliser d'abord
le [parcours Docker Compose](docker.md). Les commandes ci-dessous sont
l'alternative de developpement locale ; seules celles executees avec les
variables S3 configurees publient dans le stockage objet.

## 1. Installer les dependances

```powershell
python -m pip install -r requirements-pipeline.txt -r requirements-app.txt
```

Il faut aussi Java pour Spark. Sous Windows, le helper Hadoop deja disponible
dans `.hadoop/bin` est utilise par le projet. Si pip indique qu'aucune version
n'est disponible, verifier si PIP_NO_INDEX est active dans l'environnement ;
ce n'est pas necessairement une absence du package sur PyPI.

## 2. Jobs standalone, dans cet ordre

Pour executer toute la chaine avec arret automatique au premier echec :

```powershell
python -m scripts.run_pipeline --processing-date 2023-06-01
```

Ou lancer chaque commande vous-meme :

```powershell
python -m src.bronze --processing-date 2023-06-01
python -m src.dataops --layer bronze --processing-date 2023-06-01
python -m src.silver --processing-date 2023-06-01
python -m src.dataops --layer silver --processing-date 2023-06-01
python -m src.gold --processing-date 2023-06-01
python -m src.dataops --layer gold --processing-date 2023-06-01
```

Attendre la fin de chaque commande. Si elle echoue, ne pas lancer la suivante.
Chaque job ecrit un manifest : verifier status=SUCCESS dans
`data/reports/<couche>/<run_id>/manifest.json`. Bronze appelle l'API Chicago ;
Silver et Gold relisent des Parquets locaux. Le YAML plafonne actuellement
Bronze a deux pages. Une reussite technique peut donc rester un echantillon.

Changer uniquement --processing-date pour une autre journee. Une relance de
la meme date remplace la partition correspondante apres validation.
Les rapports historiques sont conserves sous des run_id differents.

## 3. Serveur API et frontend

Dans un autre terminal, pour les donnees reelles ecrites sous data :

```powershell
powershell -ExecutionPolicy Bypass -File scripts/serve.ps1 -Live
```

Le script lit `.env.local` s'il existe, sans afficher les valeurs, puis lance
Uvicorn au premier plan. -Live force CHICAGO_DATA_ROOT=data.
Ce fichier local contient la cle CARTO et est ignore par Git ; `.env.example`
ne contient que les noms de variables. FOURSQUARE_API_KEY est optionnelle.

Ouvrir :

- Dashboard : http://localhost:8000
- API interactive : http://localhost:8000/docs
- Etat : http://localhost:8000/api/health

Le frontend et l'API partagent le meme serveur. Aucun serveur Node ni npm n'est
necessaire. Pour arreter Uvicorn, utiliser Ctrl+C dans son terminal.

Sans script, si les variables d'environnement sont deja configurees :

```powershell
python -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

Si le port est deja utilise, choisir par exemple :

```powershell
powershell -ExecutionPolicy Bypass -File scripts/serve.ps1 -Live -Port 8001
```

## 4. Demonstration sans API Chicago

```powershell
python -m scripts.build_demo
powershell -ExecutionPolicy Bypass -File scripts/serve.ps1
```

Le fichier .env.local fourni pour la verification selectionne `data/demo`.
Ce dossier contient des donnees SYNTHETIQUES de demonstration generees avec
une graine fixe, passees dans les vrais jobs. Une banniere le signale dans
l'interface. Il ne faut jamais presenter ces chiffres comme des observations
de Chicago. -Live permet de revenir au dossier reel sans modifier les jobs.

## 5. Tests locaux

```powershell
python -m pytest tests/test_bronze.py tests/test_silver.py tests/test_gold.py tests/test_api.py -q
python -m pytest tests/test_dataops.py -q
```

Les jobs n'ont pas besoin du serveur pour tourner. Le serveur n'a pas besoin
d'une session Spark pour servir Gold. Il detecte les nouvelles publications
par leurs run_id ; rafraichir la selection dans l'interface apres un traitement.
