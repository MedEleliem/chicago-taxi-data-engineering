"""Local-only Airflow launcher with a predictable development login."""
import json
import os
from pathlib import Path

path = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
if not path.exists():
    path.write_text(json.dumps({"artefact": os.environ["CHICAGO_AIRFLOW_PASSWORD"]}), encoding="utf-8")
    path.chmod(0o600)
os.execvp("airflow", ["airflow", "standalone"])
