import json
from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from pandas import json_normalize

# 가장 중요한 기본 설정
# start_date : dag를 시작하는 기준 설정
default_args = {
    'start_date': datetime(2021, 1, 1),

}


def _processing_nft():
    assets = None
    if not len(assets):
        raise ValueError("assets is empty")

    nft = assets[0]['assets'][0]
    processed_nft = {
        'token_id': nft['token_id'],
        'name': nft['name'],
        'image_Url': nft['image_url']
    }

    processed_nft.to_csv()


with DAG(
        dag_id='nft-pipeline',  # ui 에서 보일 ID 지정
        schedule_interval='@daily',
        default_args=default_args,
        tags=['nft'],  # 나중에 찾기 쉽게
        catchup=False
) as dag:
    creating_table = SqliteOperator(
        task_id='creating_table',  # ui에서 확인 가능한 이름
        sqlite_conn_id='db_sqlite',  # UI에서 만들 수 있는 connection 정보
        sql="""
        CREATE TABLE IF NOT EXISTS nfts (
            token_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            image_url TEXT NOT NULL
        )
        """
    )

    url = "https://api.opensea.io/api/v1/assets?collection=doodles-official&limit=1"
    # headers = {"X-Api-Key": config.get('OPENSEA_API_KEY')}

    # Sensor 역시 새로운 Task
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='opensea_api',  # UI에서 connection 만들어야 함
        endpoint='api/v1/assets?collection=doodles-official&limit=1',
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:108.0) Gecko/20100101 Firefox/108.0',
            'Host': 'api.opensea.io',
            'Connection': 'Keep-alive',
            'Cookie': '__cf_bm=AfDF4fAdOgJM9VVCs4u7uezq.3YqDN6AvQBRjiHxtKA-1672055626-0-AXcktxX1dxWzAPSZa2Gy6EO2Nl0EkaXaPkye2SepGw26BLXysD4SLh3JWbg7PonR94Cn8NtuBU504lJydPKo6bA=; amp_ddd6ec=zrvP33MTAGvvuNg-dGtbFB...1gl6g52sp.1gl6g6ghr.5.1.6; __os_session=eyJpZCI6IjE5YzNhY2FlLTVkYTctNGNiYy1iZTE3LTUzZThhZTZjOGM2ZCJ9; __os_session.sig=pRhlWvdQT_zTZqpipojfp0jRJg5N6PZHpwIly2LtrGM; csrftoken=i1IX3aVlbDgd10dCO6wB6QfRU08ID25Y',
            'DNT': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'native',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'X-Api-Key': '0x2E1057DFEf94E03CbA6dF5450fEE63b2352f73cF',
            'referrer': url
        }
    )

    extract_nft = SimpleHttpOperator(
        task_id='extract_nfg',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collections=doodle'
    )
