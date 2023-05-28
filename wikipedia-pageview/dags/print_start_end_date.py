import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="print_start_end_date",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily"
)

def _print_context(**context):
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, End: {end}")

print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag
)