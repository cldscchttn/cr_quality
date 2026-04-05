FROM python:3.11-slim
WORKDIR /app

# Installa le dipendenze di sistema per psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fastapi_app/ ./fastapi_app/
COPY flask_app/ ./flask_app/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
