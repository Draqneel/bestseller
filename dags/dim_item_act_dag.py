import os
import sys

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

from datetime import timedelta

args = {
    'owner': 'Airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

PROJECT_PATH = os.environ['PROJECT_PATH']
SPARK_HOME = os.environ['SPARK_HOME']
CASSANDRA_CONNECTOR = os.environ['CASSANDRA_CONNECTOR']
PROPERTIES_PATH = os.environ['PROPERTIES_PATH']

with DAG(
    dag_id='dim_item_act',
    default_args=args,
    schedule_interval='0 0,8,16 * * *',
    start_date=days_ago(3),
    tags=['dim_item'],
) as dag:
    sys.path.append(os.path.join(SPARK_HOME, 'bin'))

    dim_item_act = BashOperator(
        task_id="dim_item_act_job",
        bash_command=f'spark-submit \
            --packages {CASSANDRA_CONNECTOR}\
            --properties-file {PROPERTIES_PATH}\
            {PROJECT_PATH}/etl/dim_item/loader.py'
    )

    
