# A-Dot API

A Flask API that wraps Airflow to run scripts on-demand with caching support.

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Root endpoint |
| `/health` | GET | Health check |
| `/scripts/{name}/run` | POST | Run a script via Airflow |
| `/scripts/{name}/status` | GET | Get script run status |

### POST /scripts/{name}/run

Run a script via Airflow DAG.

**Query Parameters:**
- `wait` (bool, default: false) - Wait for completion

**Behavior:**
- If output file is fresh (< 10 mins old), skips execution and returns cached path
- `wait=true`: Polls Airflow until DAG completes, returns result
- `wait=false`: Triggers DAG and returns immediately

**Response:**
```json
// Skipped (fresh output)
{"status": "skipped", "message": "Output is fresh (< 10 mins old)", "output_path": "./outputs/my-script/output.json"}

// Started (async)
{"status": "started", "message": "Script running in background", "dag_run_id": "manual__2025-11-21T10:30:00+00:00"}

// Completed (sync)
{"status": "finished", "dag_run_id": "manual__2025-11-21T10:30:00+00:00", "output_path": "./outputs/my-script/output.json"}
```

### GET /scripts/{name}/status

Get status of the latest run for a script.

**Response:**
```json
// Running
{"status": "running", "dag_run_id": "manual__2025-11-21T10:30:00+00:00", "started_at": "2025-11-21T10:30:00+00:00"}

// Finished
{"status": "finished", "dag_run_id": "manual__2025-11-21T10:30:00+00:00", "started_at": "2025-11-21T10:30:00+00:00", "finished_at": "2025-11-21T10:35:00+00:00", "output_path": "./outputs/my-script/output.json"}

// Idle (no runs)
{"status": "idle", "message": "No runs found"}
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRFLOW_BASE_URL` | `http://localhost:8080` | Airflow webserver URL |
| `AIRFLOW_USERNAME` | `airflow` | Airflow API username |
| `AIRFLOW_PASSWORD` | `airflow` | Airflow API password |
| `OUTPUT_DIR` | `./outputs` | Directory for script outputs |
| `FRESHNESS_SECONDS` | `600` | Cache duration (10 mins) |

## OpenAPI Documentation

| URL | Description |
|-----|-------------|
| `/openapi/openapi.json` | OpenAPI JSON schema |
| `/openapi/swagger` | Swagger UI (interactive, test APIs directly) |
| `/openapi/redoc` | ReDoc UI (clean three-panel documentation) |

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set Airflow connection (optional, defaults shown)
export AIRFLOW_BASE_URL=http://localhost:8080
export AIRFLOW_USERNAME=airflow
export AIRFLOW_PASSWORD=airflow

# Run the application
python app.py
```

The API will be available at `http://localhost:5000`.

## Running with Docker

```bash
# Build the image
docker build -t a-dot .

# Run the container
docker run -p 5000:5000 \
  -e AIRFLOW_BASE_URL=http://airflow:8080 \
  -e AIRFLOW_USERNAME=airflow \
  -e AIRFLOW_PASSWORD=airflow \
  a-dot
```

The API will be available at `http://localhost:5000`.
