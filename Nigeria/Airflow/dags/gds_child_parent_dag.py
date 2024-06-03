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
    'depends_on_past': False,
    'start_date': one_day_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='GDS_CHILD_PARENT_BAL',
          default_args=args,
          schedule_interval='0 8 * * *',
          max_active_runs=1)


with dag:
	gds_chil_par = BashOperator(task_id='gds_child_parent',bash_command='bash /nas/share05/dataOps_prod/GDS_child_parent_bal/gds_child_parent_bal.sh `date --date="-1 days" +%Y%m%d` ')
	# Set the dependencies for both possibilities
	gds_chil_par
	
