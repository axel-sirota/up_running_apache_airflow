from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.decorators import dag

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

@dag(
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='@daily',
    catchup=False,
)
def download_dag():
    download_file = BashOperator(
        task_id='download_file',
        bash_command='mkdir -p ${AIRFLOW_HOME}/downloads && wget -O ${AIRFLOW_HOME}/downloads/yelp.csv https://www.dropbox.com/scl/fi/2k8im8ftu9yk8mnqhops9/yelp.csv?rlkey=52dzmxgys0su77wb6o75vb5ab&st=lmz21rpk&dl=0'
    )

dag = download_dag()
