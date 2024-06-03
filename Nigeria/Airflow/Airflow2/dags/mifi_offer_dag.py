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
dag = DAG(dag_id='mifi_offer_dag',
          default_args=args,
          schedule_interval='0 8,10 * * *',
          max_active_runs=1)

with dag:
  mifi_offer_task = BashOperator(task_id='mifi_offer_task',bash_command='bash /nas/share05/dataOps_prod/mifi_offer/oper_mifi_offer.sh `date --date="-1 days" +%Y%m%d` ')
  # Set the dependencies for both possibilities
  mifi_offer_task
	