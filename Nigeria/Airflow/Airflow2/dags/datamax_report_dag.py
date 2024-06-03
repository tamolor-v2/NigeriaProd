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
    'email': 'kayode.ogunyemi@mtn.com',
    'email_on_failure': 'kayode.ogunyemi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=360),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='datamax_rpt_refresh',
          default_args=args,
          schedule_interval='10 6 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def end_dag():
    print('DataMax Report refreshed succesfully.')

with dag:
    datamax_rpt_refresh = BashOperator(task_id='datamax_rpt_refresh',bash_command='python3.6 /nas/share05/dataOps_dev/datamax/datamax_details_rpt.py') 

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    datamax_rpt_refresh >> end_dag

