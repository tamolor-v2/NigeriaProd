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
    'email': 'kayode.ogunyemi@mtn.com',
    'email_on_failure': 'kayode.ogunyemi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=360),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='recycled_msisdn_refresh',
          default_args=args,
#          schedule_interval='10 6 * * *'
          schedule_interval=None
)

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def end_dag():
    print('Recycled MSISDN base report refreshed succesfully.')

with dag:
    recycled_msisdn_refresh = BashOperator(task_id='recycled_msisdn_refresh',bash_command='bash /nas/share05/dataOps_dev/recycled_msisdn/recycled_msisdn_rpt.py ') 

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    recycled_msisdn_refresh >> end_dag

