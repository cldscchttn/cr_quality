FROM python:3.11-slim

WORKDIR /app

# Installa le dipendenze di sistema per psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Usa sh per espandere $PORT
CMD sh -c "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"

