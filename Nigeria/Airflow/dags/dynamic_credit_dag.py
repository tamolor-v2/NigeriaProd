import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='dynamic_credit_limit',
          default_args=args,
          schedule_interval='0 4,6,8 * * *')


with dag:
	dynamic_credit_limit = BashOperator(task_id='Dynamic_credit_Limit_report',bash_command='python3.6 /nas/share05/dataOps_prod/dynamic_credit_limit/dynamic_credit_limit.py `date --date="-1 days" +%Y%m%d` ')
	# Set the dependencies for both possibilities
	dynamic_credit_limit
	