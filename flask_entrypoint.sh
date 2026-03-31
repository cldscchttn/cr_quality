#!/bin/sh
PORT="${PORT:-5000}"
echo "Starting Flask on port $PORT"
exec python flask_app/app.py
