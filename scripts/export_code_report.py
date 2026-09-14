"""Generate a readable source appendix without credentials or historical modules."""
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    files = ["src/common.py", "src/object_store.py", "src/bronze/__main__.py", "src/silver/__main__.py",
             "src/gold/__main__.py", "src/dataops/__main__.py", "src/api/main.py", "src/api/service.py",
             "frontend/index.html", "frontend/css/style.css", "frontend/js/app.js",
             "config/chicago_taxi.yml", "dags/chicago_taxi_batch_pipeline.py",
             "scripts/run_pipeline.py", "scripts/run_stage.py", "scripts/start_airflow.py", "scripts/serve.ps1", "scripts/build_demo.py",
             "scripts/prepare_validation.py", "scripts/check_frontend.py", "scripts/check_release.py",
             "scripts/inspect_release.py",
             "tests/conftest.py", "tests/test_bronze.py", "tests/test_silver.py", "tests/test_gold.py",
             "tests/test_api.py", "tests/test_dataops.py", "tests/test_object_store.py",
             "tests/test_s3_integration.py",
             "compose.yml", "docker/api.Dockerfile", "docker/airflow.Dockerfile"]
    sections = ["# Code complet de la version active\n\n"
                "Instantane genere depuis les sources. Les fichiers executables restent la reference.\n\n"
                "Lire les guides architecture, bronze, silver, gold, api, frontend et dataops pour\n"
                "l'explication des fonctions. Les secrets et les modules historiques sont exclus.\n"]
    languages = {".py": "python", ".yml": "yaml", ".js": "javascript", ".ps1": "powershell"}
    for relative in files:
        path = root / relative
        language = languages.get(path.suffix, path.suffix.lstrip("."))
        sections.append(f"## {relative}\n\n```{language}\n{path.read_text(encoding='utf-8').rstrip()}\n```\n")
    destination = root / "docs/reports/code_complet.md"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(sections), encoding="utf-8")
    print(destination)


if __name__ == "__main__":
    main()
