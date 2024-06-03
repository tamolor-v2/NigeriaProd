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
    'depends_on_past':False,
    'start_date': today,
    'email': 'joshua.avbuere@mtn.com',
    'email_on_failure': 'joshua.avbuere@mtn.com',
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='smartweb_tranx',
          default_args=args,
          schedule_interval='15 08 * * *'
#         schedule_interval=None

)


def end_dag():
    print('MYMTN Smart Web transaction Report added succesfully.')

with dag:
    smartweb_tranx_write = BashOperator(task_id='data_sub_perf',bash_command ='bash /nas/share05/dataOps_dev/smartweb_tranx/smartweb_tranx.sh  `date --date="-1 days" +%Y%m%d`' )

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    smartweb_tranx_write >> end_dag