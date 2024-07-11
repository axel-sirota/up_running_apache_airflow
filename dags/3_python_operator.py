from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

@dag(
    default_args=default_args,
    description='A DAG to download, process, and clean up Yelp data',
    schedule_interval='@daily',
    catchup=False,
)
def yelp_data_pipeline():
    download_file = BashOperator(
        task_id='download_file',
        bash_command='mkdir -p ${AIRFLOW_HOME}/downloads && wget -O ${AIRFLOW_HOME}/downloads/yelp.csv https://www.dropbox.com/scl/fi/2k8im8ftu9yk8mnqhops9/yelp.csv?rlkey=52dzmxgys0su77wb6o75vb5ab&st=lmz21rpk&dl=0'
    )

    def process_data():
        import os
        base_path = os.environ['AIRFLOW_HOME']
        df = pd.read_csv(f'{base_path}/downloads/yelp.csv')
        df_filtered = df[['text', 'stars']]
        df_filtered.to_csv(f'{base_path}/downloads/processed_yelp.csv', index=False)

    process_file = PythonOperator(
        task_id='process_file',
        python_callable=process_data
    )

    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm -rf ${AIRFLOW_HOME}/downloads/processed_yelp.csv'
    )

    download_file >> process_file >> cleanup

dag = yelp_data_pipeline()
