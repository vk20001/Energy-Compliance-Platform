from __future__ import annotations

import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="smard_price_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["cisaf", "smard", "prices"],
    doc_md="""
    ## SMARD Price DAG
    Fetches German day-ahead wholesale electricity prices from the SMARD API
    and uploads them to Databricks (workspace.energy_compliance.smard_wholesale_prices).
    Runs weekly. Idempotent -- re-fetching existing data overwrites in place.
    """,
)
def smard_price_dag():

    @task()
    def fetch_and_upload_prices():
        import os

        project_root = "/home/vaishu19/energy-compliance-platform"
        scripts_dir = os.path.join(project_root, "scripts")

        result = subprocess.run(
            [sys.executable, os.path.join(scripts_dir, "fetch_smard_prices.py")],
            capture_output=True,
            text=True,
            cwd=project_root,
        )

        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"fetch_smard_prices.py failed with return code {result.returncode}")

        print("SMARD price fetch and upload completed successfully.")

    fetch_and_upload_prices()


smard_price_dag()
