#! /bin/bash

docker run \
-it \
-p 8080:8080 \
-v /Users/yjkim-studio/src/apache-airflow/data-pipeline/dags:/opt/airflow/dags \
--entrypoint=/bin/bash \
--name airflow \
apache/airflow:2.6.0-python3.8 \
-c '( \
airflow db init && \
airflow users create --username admin --password admin --firstname YJ --lastname Kim --role Admin --email admin@example.org \
); \
airflow webserver & \
airflow scheduler \
'