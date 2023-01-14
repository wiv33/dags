import json
from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize

# 가장 중요한 기본 설정
# start_date : dag를 시작하는 기준 설정
default_args = {
    'start_date': datetime(2021, 1, 1),
}

with DAG(
        dag_id='publish-powerball-5',  # ui 에서 보일 ID 지정
        schedule_interval='*/5 * * * *',
        default_args=default_args,
        tags=['powerball'],  # 나중에 찾기 쉽게
        catchup=False
) as dag:
    BashOperator(
        task_id='publish_5',
        bash_command='python /Users/auto/PycharmProjects/A-Learning-python/my-kafka/_000_basic/_003_xyz_connect'
                     '/_001_publish_5.py'
    )
