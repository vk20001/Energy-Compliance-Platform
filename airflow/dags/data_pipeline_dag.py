from __future__ import annotations

import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="data_pipeline_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["cisaf", "dbt", "compliance"],
    doc_md="""
    ## Data Pipeline DAG
    Runs dbt build against Databricks to refresh all gold layer models,
    then queries facility_compliance_summary and logs compliance status
    for all 8 facilities.
    Task order: dbt_build -> compliance_check
    """,
)
def data_pipeline_dag():

    @task()
    def dbt_build():
        import os

        project_root = "/home/vaishu19/energy-compliance-platform"
        dbt_project_dir = os.path.join(project_root, "energy_dbt")
        dbt_executable = os.path.join(project_root, "venv/bin/dbt")

        result = subprocess.run(
            [dbt_executable, "build", "--project-dir", dbt_project_dir, "--profiles-dir", os.path.expanduser("~/.dbt")],
            capture_output=True,
            text=True,
            cwd=dbt_project_dir,
        )

        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"dbt build failed with return code {result.returncode}")

        print("dbt build completed successfully.")

    @task()
    def compliance_check():
        import os
        from databricks import sql

        server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "dbc-174e65df-4c24.cloud.databricks.com")
        http_path = os.environ.get("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/d7ae726d9291a51f")
        access_token = os.environ.get("DATABRICKS_TOKEN")

        if not access_token:
            raise RuntimeError("DATABRICKS_TOKEN environment variable not set")

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        facility_name,
                        compliance_status,
                        total_subsidy_eur,
                        compliance_pct,
                        flexibility_bonus_eligible
                    FROM workspace.energy_compliance.facility_compliance_summary
                    ORDER BY compliance_pct ASC, facility_name
                """)
                rows = cursor.fetchall()

        print("=" * 60)
        print("FACILITY COMPLIANCE SUMMARY")
        print("=" * 60)
        at_risk = []
        for row in rows:
            print(f"{row[0]}: status={row[1]}, subsidy=EUR {row[2]:,.2f}, compliance={row[3]:.1f}%, flexibility_bonus={row[4]}")
            if row[1] == "AT_RISK" or (row[3] is not None and row[3] < 50):
                at_risk.append(row[0])

        if at_risk:
            print(f"\nWARNING: {len(at_risk)} facility/facilities at risk: {', '.join(at_risk)}")
        else:
            print("\nAll facilities within compliance thresholds.")
        print("=" * 60)

    dbt_run = dbt_build()
    check = compliance_check()
    dbt_run >> check


data_pipeline_dag()
