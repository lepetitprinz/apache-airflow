import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="non_atomic_send",
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 1, 1),
    catchup=True
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events/{{ds}}.json "
        "http://events_api:5000/events?"
        "start_date={{ds}}&"
        "end_date={{next_ds}}"
    ),
    dag=dag
)

def _calculae_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["template_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

    # Sending an eamil after writing to CSV creates two pieces of work in a single function, which breaks the atomicity of the task
    _email_stats(stats, email="user@example.com")

def _email_stats(stats, email):
    # Send an email
    print(f"Sending stats to {email}")

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculae_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv",
    },
    dag=dag
)

fetch_events >> calculate_stats