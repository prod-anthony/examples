"""
DAG to fetch BTC price and write to file.
"""

import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _fetch_btc_price(**context):
    """Fetch BTC price from CoinGecko API and write to file."""
    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "bitcoin", "vs_currencies": "usd"},
        timeout=10,
    )
    response.raise_for_status()

    price = response.json()["bitcoin"]["usd"]
    timestamp = datetime.now().isoformat()

    output = {
        "timestamp": timestamp,
        "btc_price_usd": price,
    }

    output_path = f"/opt/airflow/artifacts/btc_price_{context['ds']}.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"BTC price: ${price} written to {output_path}")
    return price


with DAG(
    dag_id="btc_price",
    default_args=default_args,
    description="Fetch BTC price and save to artifacts",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "btc"],
) as dag:

    fetch_price = PythonOperator(
        task_id="fetch_btc_price",
        python_callable=_fetch_btc_price,
    )
