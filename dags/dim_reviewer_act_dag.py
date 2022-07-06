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
    dag_id='dim_reviewer_act',
    default_args=args,
    schedule_interval='0 2,10,18 * * *',
    start_date=days_ago(2),
    tags=['dim_reviewer'],
) as dag:
    sys.path.append(os.path.join(SPARK_HOME, 'bin'))

    dim_reviewer_act = BashOperator(
        task_id="dim_reviewer_act_job",
        bash_command=f'spark-submit \
            --packages {CASSANDRA_CONNECTOR}\
            --properties-file {PROPERTIES_PATH}\
            {PROJECT_PATH}/etl/dim_reviewer/loader.py'
    )

    
