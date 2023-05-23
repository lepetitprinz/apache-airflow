import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="templated_path", 
    schedule_interval="@daily",  
    start_date=dt.datetime(2023, 1, 1), 
    end_date=dt.datetime(2023, 1, 7)
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events/{{ds}}.json "  # Response written to templated filename
        "http://events_api:5100/events?"
        "start_date={{ds}}&" # ds provides YYYY-MM-DD formatted
        "end_date={{next_ds}}" # next_ds provides the same for next_execution_date
    ),
    dag=dag
)

# Calculates event statistics
def _calculate_stats(**context):
    # Retrieve the templated values from the templates_dict object
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    # Load the events and calculate the required statistics
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        # Pass the values that we want to be templated
        "input_path": "/data/events/{{ds}}.json", 
        "output_path": "/data/stats/{{ds}}.csv"
        },
    dag=dag,
)

# Set order of execution
fetch_events >> calculate_stats