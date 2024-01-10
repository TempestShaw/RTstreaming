from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'air_scholar',
    'start_date': datetime(2023, 8, 3, 10, 00)
}


def get_data():
    import json
    import requests
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent = 3)) -- check data with good format
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])}" + ' ' + f"{location['street']['name']}" + ', ' + f"{location['city']}" +', '+ f"{location['state']}" + ', ' + f"{location['country']}"

def stream_data():
    res = get_data()
    format = format_data(res)
    print(format)

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='api_stream',
        python_callable=stream_data
    )

stream_data()