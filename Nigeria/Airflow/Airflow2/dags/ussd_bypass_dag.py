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
    'retries': 4,
    'retry_delay': timedelta(minutes=3),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='USSD_BYPASS_REPORT',
          default_args=args,
          schedule_interval='10 8 * * *')
def init_dag():
    print('Banks USSD logs extracted initializing...')

def end_dag():
    print('Banks USSD logs extracted succesfully')

with dag:
    incoming_stream = BashOperator(task_id='ussd_bypass_rpt',bash_command='bash /nas/share05/dataOps_prod/ussd_bypass_rpt/ussd_bypass_rpt.sh  `date --date"=-1 days" +%Y%m%d` ',dag=dag,run_as_user = 'daasuser')
    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)
    init_dag = PythonOperator(task_id='init_dag',python_callable=init_dag)
    # Set the dependencies for both possibilities
    init_dag >> incoming_stream >> end_dag