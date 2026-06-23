"""Example DAG to verify Airflow is working."""

from datetime import datetime

from airflow.sdk import DAG, task


@task
def hello():
    print("Hello from Airflow!")
    return "done"


with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
):
    hello()
