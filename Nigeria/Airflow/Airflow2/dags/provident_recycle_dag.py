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
    'retries': 4,
    'retry_delay': timedelta(minutes=15),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='provident_recycled_base',
          default_args=args,
          schedule_interval='10 6 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def end_dag():
    print('provident Recycled base report refreshed succesfully.')

with dag:
    fetch_recycled_base = BashOperator(task_id='fetch_recycled_base',bash_command='bash /nas/share05/ops/mtnops/prov_recycle_fetch.sh ')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    fetch_recycled_base >> end_dag

