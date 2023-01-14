from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(
        dag_id="taxi-price-pipeline",
        schedule_interval="@daily",
        default_args=default_args,
        tags=['spark'],
        catchup=False
) as dag:
    # preprecess
    preprocess = SparkSubmitOperator(
        application='/Users/auto/github/data-engineering-fastcampus/src/main/scala/_02/preprocess.py',
        task_id='preprocess',
        conn_id='spark_local'
    )

    # tune hyperparameter
    tune_hyperparameter = SparkSubmitOperator(
        application='/Users/auto/github/data-engineering-fastcampus/src/main/scala/_02/tune_hyperparameter.py',
        task_id='tune_hyperparameter',
        conn_id='spark_local'
    )

    # train model
    train_model = SparkSubmitOperator(
        application='/Users/auto/github/data-engineering-fastcampus/src/main/scala/_02/train_model.py',
        task_id='train_model',
        conn_id='spark_local'
    )

    preprocess >> tune_hyperparameter >> train_model
