from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

import logging

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}
# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = DockerOperator(
        task_id='streaming_task',
        image='playwright-scraper',  # Use your Playwright Docker image
        api_version='auto',
        auto_remove='success',
        command="python kafka_stream.py",  # Ensure this is the correct command to run your script
        docker_url="unix://var/run/docker.sock",  # Assumes Docker is running on the host
        network_mode="bridge"
    )
