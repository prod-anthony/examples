import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests


class AirflowClient:
    """Client to interact with Airflow REST API (v2 for Airflow 3.x)."""

    AIRFLOW_STATE_MAP = {
        "queued": "running",
        "running": "running",
        "success": "finished",
        "failed": "failed",
    }

    def __init__(
        self,
        base_url: str = None,
        username: str = None,
        password: str = None,
    ):
        self.base_url = (base_url or os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")).rstrip("/")
        self.username = username or os.getenv("AIRFLOW_USERNAME", "airflow")
        self.password = password or os.getenv("AIRFLOW_PASSWORD", "airflow")
        self._token: Optional[str] = None
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _get_token(self) -> str:
        """Get JWT token from Airflow auth endpoint."""
        if self._token:
            return self._token

        url = f"{self.base_url}/auth/token"
        response = requests.post(
            url,
            json={"username": self.username, "password": self.password},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        self._token = response.json()["access_token"]
        return self._token

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}/api/v2{path}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make authenticated request to Airflow API."""
        token = self._get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"

        response = self.session.request(method, self._api_url(path), headers=headers, **kwargs)

        # Retry once if token expired
        if response.status_code == 401:
            self._token = None
            token = self._get_token()
            headers["Authorization"] = f"Bearer {token}"
            response = self.session.request(method, self._api_url(path), headers=headers, **kwargs)

        response.raise_for_status()
        return response

    def trigger_dag(self, dag_id: str, conf: dict = None) -> dict:
        """Trigger a DAG run."""
        payload = {
            "logical_date": datetime.now(timezone.utc).isoformat(),
            "conf": conf or {},
        }
        response = self._request("POST", f"/dags/{dag_id}/dagRuns", json=payload)
        return response.json()

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        """Get status of a specific DAG run."""
        response = self._request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        return response.json()

    def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> dict:
        """Get mapped status of a DAG run."""
        dag_run = self.get_dag_run(dag_id, dag_run_id)
        airflow_state = dag_run.get("state", "unknown")

        return {
            "status": self.AIRFLOW_STATE_MAP.get(airflow_state, airflow_state),
            "airflow_state": airflow_state,
            "dag_run_id": dag_run_id,
            "start_date": dag_run.get("start_date"),
            "end_date": dag_run.get("end_date"),
        }

    def wait_for_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        poll_interval: float = 2.0,
        timeout: float = 300.0,
    ) -> dict:
        """Poll until DAG run completes or timeout."""
        start_time = time.time()

        while True:
            status = self.get_dag_run_status(dag_id, dag_run_id)

            if status["status"] in ("finished", "failed"):
                return status

            if time.time() - start_time > timeout:
                return {
                    **status,
                    "status": "timeout",
                    "message": f"Timed out after {timeout}s",
                }

            time.sleep(poll_interval)

    def get_latest_dag_run(self, dag_id: str) -> Optional[dict]:
        """Get the most recent DAG run."""
        params = {"order_by": "-start_date", "limit": 1}
        response = self._request("GET", f"/dags/{dag_id}/dagRuns", params=params)

        dag_runs = response.json().get("dag_runs", [])
        return dag_runs[0] if dag_runs else None
