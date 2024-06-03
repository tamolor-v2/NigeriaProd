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
    'depends_on_past':False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='eSIM_Activation',
          default_args=args,
          schedule_interval='0 4,6,8 * * *')


with dag:
	eSIM_Activation = BashOperator(task_id='eSIM_Activation_report',bash_command='bash /nas/share05/dataOps_prod/eSIM_activation/eSIM_activation.sh `date --date="-1 days" +%Y%m%d` && bash /nas/share05/dataOps_prod/eSIM_activation/eSIM_activation.sh `date --date="-2 days" +%Y%m%d` && bash /nas/share05/dataOps_prod/eSIM_activation/eSIM_activation.sh `date --date="-3 days" +%Y%m%d` ')
	# Set the dependencies for both possibilities
	eSIM_Activation
	
