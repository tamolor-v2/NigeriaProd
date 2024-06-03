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
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

# instantiate dag
dag = DAG(dag_id='joshua_loop',
          default_args=args,
#          schedule_interval='* * */24 * *'
          schedule_interval=None

)


def end_dag():
    print('Joshua added succesfully.')

with dag:
    joshua_write = BashOperator(task_id='joshua_loop',bash_command ='/nas/share05/dataOps_dev/mnp_incentive/joshua_loop.sh ' )

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    joshua_write >> end_dag
