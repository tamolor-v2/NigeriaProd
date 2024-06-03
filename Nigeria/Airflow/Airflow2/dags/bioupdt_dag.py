import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

# instantiate dag
dag = DAG(dag_id='BIOUPDATE_DAG',
          default_args=args,
          schedule_interval='0 1 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

with dag:
    kick_off_dag = BashOperator(task_id='kick_off_dag',depends_on_past=False,
    #bash_command='/nas/share05/ops/mtnops/data_ingestion.py -l 10 BIOUPDT',
    bash_command='echo hello',
    dag=dag,
     run_as_user = 'daasuser')
    # Set the dependencies for both possibilities
    kick_off_dag
