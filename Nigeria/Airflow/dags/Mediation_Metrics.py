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
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

# instantiate dag
dag = DAG(dag_id='New_mtnn_mediation_metrics',
          default_args=args,
          schedule_interval='0 4 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


with dag:
    kick_off_dag = BashOperator(task_id='New_mtnn_mediation_metrics',bash_command='python3.6 /nas/share05/ops/mtnops/man_Mediation_metrics.py `date --date="-1 days" +%Y%m%d` ',     dag=dag,
     run_as_user = 'daasuser')
    device_mapper = BashOperator(task_id='device_mapper',bash_command='bash /nas/share05/ops/mtnops/spool_OWC1.sh ',     dag=dag,
     run_as_user = 'daasuser')
    agent_user_bib = BashOperator(task_id='agent_user_bib',bash_command='bash /nas/share05/ops/mtnops/spool_OWC3.sh ',     dag=dag,
     run_as_user = 'daasuser')
    kick_off_dag >> device_mapper >> agent_user_bib


