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
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='extract_kitchen_Actv',
          default_args=args,
          schedule_interval='0 11 * * *',
          max_active_runs=1)


with dag:
	extract_kit_actv = BashOperator(task_id='extract_kitchen_actv_base',bash_command='bash /nas/share05/dataOps_prod/extract_kitchen_actv/extract_kitchen_actv.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%d%m%Y` ')
	# Set the dependencies for both possibilities
	extract_kit_actv
	
