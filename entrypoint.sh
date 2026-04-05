#!/bin/sh
PORT="${PORT:-8000}"
echo "Starting uvicorn on port $PORT"
exec uvicorn fastapi_app.main:app --host 0.0.0.0 --port "$PORT"
