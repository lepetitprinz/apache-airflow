import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="atomic_send",
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

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculae_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv",
    },
    dag=dag
)

def _email_stats(stats, email):
    # Send an email
    print(f"Sending stats to {email}")

def _send_stats(email, **context):
    stats = pd.read_csv(context["templates_dict"]["stats_path"])
    _email_stats(stats, email=email)

send_stats = PythonOperator(
    task_id="send_stats",
    python_callable=_send_stats,
    op_kwargs={
        "email": "user@example.com"
    },
    templates_dict={
        "stats_path": "/data/stats/{{ds}}.csv"
    },
    dag=dag
)

fetch_events >> calculate_stats >> send_stats