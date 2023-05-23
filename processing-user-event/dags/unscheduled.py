from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="unscheduled", 
    start_date=datetime(2023, 5, 20), 
    schedule_interval=None
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events.json http://events_api:5100/events"
    ),
    dag=dag
)

# Calculates event statistics
def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(exist_ok=True)

    # Load the events and calculate the required statistics
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/data/events.json", 
        "output_path": "/data/stats.csv"
        },
    dag=dag,
)

# Set order of execution
fetch_events >> calculate_stats