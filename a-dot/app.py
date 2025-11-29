import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from flask_openapi3 import OpenAPI, Info
from pydantic import BaseModel, Field

from airflow_client import AirflowClient

info = Info(title="A-Dot API", version="1.0.0")
app = OpenAPI(__name__, info=info)

# Configuration
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./outputs"))
ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "/opt/airflow/artifacts"))
FRESHNESS_SECONDS = int(os.getenv("FRESHNESS_SECONDS", 600))  # 10 minutes

# Initialize Airflow client
airflow = AirflowClient()


# Pydantic models for OpenAPI
class ScriptPath(BaseModel):
    script_name: str = Field(..., description="Name of the script/DAG to run")


class RunQuery(BaseModel):
    wait: bool = Field(False, description="Wait for script to complete")


class RunBody(BaseModel):
    conf: Optional[dict] = Field(None, description="Configuration to pass to the DAG")
    output_filename: Optional[str] = Field(None, description="Artifact filename (required when wait=true)")


class RunResponse(BaseModel):
    status: str
    message: Optional[str] = None
    output_path: Optional[str] = None
    dag_run_id: Optional[str] = None
    conf: Optional[dict] = None
    content: Optional[dict] = Field(None, description="File content when wait=true")


class StatusResponse(BaseModel):
    status: str
    dag_run_id: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    output_path: Optional[str] = None


class ArtifactPath(BaseModel):
    file_name: str = Field(..., description="Name of the artifact file")


class ArtifactResponse(BaseModel):
    file_name: str
    content: dict


class ArtifactListResponse(BaseModel):
    files: list[dict]
    total: int


def get_output_path(script_name: str) -> Path:
    """Get the output file path for a script."""
    return OUTPUT_DIR / script_name / "output.json"


def is_output_fresh(script_name: str) -> bool:
    """Check if output file exists and was modified within FRESHNESS_SECONDS."""
    output_path = get_output_path(script_name)

    if not output_path.exists():
        return False

    mtime = datetime.fromtimestamp(output_path.stat().st_mtime, tz=timezone.utc)
    age = (datetime.now(tz=timezone.utc) - mtime).total_seconds()

    return age < FRESHNESS_SECONDS


@app.get("/")
def index():
    """Root endpoint"""
    return {"message": "Welcome to A-Dot API"}


@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.post("/scripts/<script_name>/run", responses={200: RunResponse})
def run_script(path: ScriptPath, query: RunQuery, body: RunBody):
    """
    Run a script via Airflow.

    - If output is fresh (< 10 mins), skip execution and return cached path
    - wait=true: Wait for completion and return file content (requires output_filename)
    - wait=false: Trigger and return immediately
    - conf: Optional configuration to pass to the DAG
    - output_filename: Required when wait=true, will be added to conf

    Example request body:
    {
        "output_filename": "results.json",
        "conf": {
            "input_file": "data.csv",
            "threshold": 100,
            "user_id": 123
        }
    }
    """
    script_name = path.script_name
    conf = body.conf or {}
    output_filename = body.output_filename

    # Validate: output_filename is required when wait=true
    if query.wait and not output_filename:
        return {"status": "error", "message": "output_filename is required when wait=true"}, 400

    # Validate filename if provided
    if output_filename and not is_valid_filename(output_filename):
        return {"status": "error", "message": "Invalid output_filename. Only alphanumeric, underscore, and hyphen allowed."}, 400

    # Add output_filename to conf if provided
    if output_filename:
        conf["output_filename"] = f"{output_filename}.json"

    # Check freshness
    if is_output_fresh(script_name):
        return {
            "status": "skipped",
            "message": "Output is fresh (< 10 mins old)",
            "output_path": str(get_output_path(script_name)),
            "conf": conf,
        }

    try:
        # Trigger DAG with configuration
        dag_run = airflow.trigger_dag(script_name, conf=conf)
        dag_run_id = dag_run["dag_run_id"]

        if query.wait:
            # Wait for completion
            result = airflow.wait_for_dag_run(script_name, dag_run_id)

            response = {
                "status": result["status"],
                "dag_run_id": dag_run_id,
                "conf": conf,
            }

            # If successful, read and return the artifact content
            if result["status"] == "finished":
                artifact_path = ARTIFACTS_DIR / f"{output_filename}.json"

                if artifact_path.exists():
                    try:
                        with open(artifact_path) as f:
                            content = json.load(f)
                        response["content"] = content
                        response["output_path"] = str(artifact_path)
                    except json.JSONDecodeError:
                        response["message"] = "Output file is not valid JSON"
                    except Exception as e:
                        response["message"] = f"Error reading output file: {str(e)}"
                else:
                    response["message"] = "Output file not found in artifacts directory"

            return response
        else:
            # Return immediately
            return {
                "status": "started",
                "message": "Script running in background",
                "dag_run_id": dag_run_id,
                "conf": conf,
            }

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


@app.get("/scripts/<script_name>/status", responses={200: StatusResponse})
def get_script_status(path: ScriptPath):
    """
    Get the status of the latest run for a script.

    Returns: idle, running, finished, or failed with timestamps.
    """
    script_name = path.script_name

    try:
        latest_run = airflow.get_latest_dag_run(script_name)

        if not latest_run:
            return {"status": "idle", "message": "No runs found"}

        dag_run_id = latest_run["dag_run_id"]
        status = airflow.get_dag_run_status(script_name, dag_run_id)

        response = {
            "status": status["status"],
            "dag_run_id": dag_run_id,
            "started_at": status.get("start_date"),
        }

        if status["status"] == "finished":
            response["finished_at"] = status.get("end_date")
            response["output_path"] = str(get_output_path(script_name))

        return response

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


def is_valid_filename(file_name: str) -> bool:
    """Validate filename - only alphanumeric, underscore, and hyphen allowed."""
    return bool(re.match(r"^[\w\-]+$", file_name))


@app.get("/artifacts/<file_name>", responses={200: ArtifactResponse})
def get_artifact(path: ArtifactPath):
    """
    Get the content of an artifact file.

    Returns the JSON content of the specified file from /opt/airflow/artifacts.
    File name should only contain alphanumeric, underscore, or hyphen.
    Extension .json is automatically appended.
    """
    file_name = path.file_name

    if not is_valid_filename(file_name):
        return {"status": "error", "message": "Invalid filename. Only alphanumeric, underscore, and hyphen allowed."}, 400

    file_path = ARTIFACTS_DIR / f"{file_name}.json"

    if not file_path.exists():
        return {"status": "error", "message": "File not found"}, 404

    try:
        with open(file_path) as f:
            content = json.load(f)

        return {"file_name": file_name, "content": content}

    except json.JSONDecodeError:
        return {"status": "error", "message": "File is not valid JSON"}, 400
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


@app.get("/artifacts", responses={200: ArtifactListResponse})
def list_artifacts():
    """
    List all files in the artifacts directory.

    Returns a list of files with metadata (name, size, modified time).
    """
    try:
        if not ARTIFACTS_DIR.exists():
            return {"files": [], "total": 0}

        files = []
        for file_path in sorted(ARTIFACTS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if file_path.is_file():
                stat = file_path.stat()
                files.append({
                    "name": file_path.name,
                    "name_without_ext": file_path.stem,
                    "size": stat.st_size,
                    "size_human": format_size(stat.st_size),
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "is_json": file_path.suffix == ".json",
                })

        return {"files": files, "total": len(files)}

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


if __name__ == "__main__":
    app.run(debug=True)
