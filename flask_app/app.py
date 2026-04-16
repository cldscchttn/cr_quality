import logging
import os

import requests
from flask import Flask, jsonify, render_template

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = Flask(__name__)

FASTAPI_BASE_URL = os.getenv(
    "FASTAPI_BASE_URL",
    "https://fastapi-75q7-production.up.railway.app",
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
def index():
    logger.info("GET /")
    return render_template("index.html")

@app.get("/provaapi")
def prova_api():
    logger.info("GET /provaapi")
    return render_template("provaapi.html")

@app.get("/tassigrezzi")
def tassi_grezzi_page():
    logger.info("GET /tassigrezzi")
    return render_template("tassi_grezzi.html")

@app.get("/tassistd")
def tassi_std_page():
    logger.info("GET /tassistd")
    return render_template("tassi_std.html")


@app.get("/api/data")
def api_data():
    """Proxy to FastAPI /api/test and return the JSON result."""
    url = f"{FASTAPI_BASE_URL}/api/test"
    logger.info("GET /api/data — forwarding request to %s", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("GET /api/data — received response: %s", data)
        return jsonify(data)
    except requests.exceptions.ConnectionError as exc:
        logger.error("GET /api/data — connection error: %s", exc)
        return jsonify({"error": "Could not connect to FastAPI backend", "detail": str(exc)}), 502
    except requests.exceptions.Timeout:
        logger.error("GET /api/data — request timed out")
        return jsonify({"error": "Request to FastAPI backend timed out"}), 504
    except requests.exceptions.HTTPError as exc:
        logger.error("GET /api/data — HTTP error from FastAPI: %s", exc)
        return jsonify({"error": "FastAPI returned an error", "detail": str(exc)}), exc.response.status_code
    except Exception as exc:
        logger.error("GET /api/data — unexpected error: %s", exc)
        return jsonify({"error": "Unexpected error", "detail": str(exc)}), 500


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    logger.info("Starting Flask on port %d", port)
    app.run(host="0.0.0.0", port=port)
