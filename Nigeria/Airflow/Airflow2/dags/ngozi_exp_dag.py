import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

one_day_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': one_day_ago,
    'email': 'ngozichukwu.okwechime@mtn.com',
    'email_on_failure': 'ngozichukwu.okwechime@mtn.com',
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='ngozi_exp',
          default_args=args,
#          schedule_interval='0 * * * *',
          schedule_interval=None,
          max_active_runs=1)

with dag:
  append_name_task = BashOperator(task_id='append_name_task',bash_command='bash /home/ngozokwe/append_name.sh',run_as_user = 'daasuser')
  # Set the dependencies for both possibilities
  append_name_task
	
