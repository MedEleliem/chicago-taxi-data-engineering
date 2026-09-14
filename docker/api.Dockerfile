FROM python:3.12-slim-bookworm
WORKDIR /opt/chicago
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt
COPY src/__init__.py src/__init__.py
COPY src/api/ src/api/
COPY src/object_store.py src/object_store.py
COPY frontend/ frontend/
RUN useradd --uid 10001 --create-home app && mkdir -p data && chown app:app data
USER app
CMD ["python", "-m", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
