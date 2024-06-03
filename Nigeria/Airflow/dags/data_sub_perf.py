import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os


today = datetime.now() - timedelta(days=7)

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': today,
    'email': 'joshua.avbuere@mtn.com', 'ngozichukwu.okwechime@mtn.com'
    'email_on_failure': 'joshua.avbuere@mtn.com', 'ngozichukwu.okwechime@mtn.com'
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='data_sub_perf',
          default_args=args,
          schedule_interval='15 08 * * *'
#         schedule_interval=None

)


def end_dag():
    print('Data Subscriber Performance Report added succesfully.')

with dag:
    data_sub_perf_write = BashOperator(task_id='data_sub_perf',bash_command ='bash /nas/share05/dataOps_dev/data_sub_perf/data_sub_perf.sh  `date --date="-1 days" +%Y%m%d`' )

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    data_sub_perf_write >> end_dag