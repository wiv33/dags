from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# 가장 중요한 기본 설정
# start_date : dag를 시작하는 기준 설정
default_args = {
    'start_date': datetime(2021, 1, 1),
}

USER_ID = 'nono1'
NUMBER = 1

with DAG(
        dag_id=f'{USER_ID}-bet-powerball-5-{NUMBER}',  # ui 에서 보일 ID 지정
        schedule_interval='@once',
        default_args=default_args,
        tags=['powerball', USER_ID],  # 나중에 찾기 쉽게
        catchup=False
) as dag:
    BashOperator(task_id=f'bet_5_{NUMBER}',
                 bash_command=f'python /Users/auto/PycharmProjects/A-Learning-python/Crawling/_000_basic'
                              f'/_003_extract_number/_007_sbuks_bet.py {USER_ID}')
