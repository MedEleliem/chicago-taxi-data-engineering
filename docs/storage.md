# Publications dans le stockage objet

`src/object_store.py` est le seul module S3. Il ne transforme pas les trajets.
`scripts/run_stage.py` fait le lien entre les jobs existants et ce stockage.

## Ecriture

Apres reussite du job et de DataOps, publish transfere le Parquet, les rapports,
le manifest, l'audit et les logs dans un prefixe unique :

```text
s3://chicago-taxi/bronze/processing_date=2023-06-01/runs/BRZ_.../
s3://chicago-taxi/silver/processing_date=2023-06-01/runs/SLV_.../
s3://chicago-taxi/gold/processing_date=2023-06-01/runs/GLD_.../
```

Chaque fichier est inventorie par chemin relatif, taille et SHA-256. Les objets
du run sont crees avec IfNoneMatch : ils ne remplacent pas ceux d'un autre run.
La taille et les metadonnees sont relues apres envoi. publication.json decrit
le lot. Le pointeur latest.json n'est remplace qu'une fois l'envoi termine,
avec IfMatch sur l'ETag observe avant l'envoi (ou IfNoneMatch pour le premier).
Un conflit de publication echoue au lieu d'ecraser silencieusement ce pointeur.

Une panne peut laisser des objets sans pointeur, jamais une publication annoncee
complete avec un envoi encore en cours. Les anciens runs S3 restent disponibles,
contrairement aux partitions locales de travail qui sont remplacees. Il n'y a
pas de purge automatique ni de transaction entre les trois couches.

## Lecture

restore telecharge le snapshot source et verifie chaque SHA-256. Il remplace
la partition de travail dans son ensemble, pour ne pas conserver d'anciens
part-*.parquet. Les rapports de lineage sont restaures aussi. Les chemins de
ces manifests sont ajustes a la racine de travail ; les valeurs metier ne
sont pas modifiees.

Pour l'API, gold_publications telecharge des snapshots immuables dans un cache
separe. Le marqueur _download_complete.json est ecrit apres les verifications.
Les requetes consomment le snapshot identifie par son run_id. Chaque publication
Gold emporte les rapports exacts Bronze/Silver dont elle depend, meme apres une
nouvelle ingestion sur la meme date.

## Arbitrage volontaire

Spark travaille sur disque local, puis boto3 materialise les couches dans S3.
Ce choix evite la configuration Hadoop S3A et ses JAR additionnels pour un
batch local. Ce n'est pas un simple backup : les etapes aval et l'API relisent
S3. Le cout est une copie locale, du disque temporaire et un transfert par
etape. Pour de gros volumes distribues, preferer S3A et un format transactionnel.

La publication utilise des preconditions S3, pas un verrou distribue de pipeline.
Airflow serialise les runs ; les lancements manuels concurrents restent interdits.
Les snapshots sans pointeur et caches anciens doivent etre nettoyes par une
politique de retention future. Ne pas supprimer les rapports sources utilises
par une publication Gold.
