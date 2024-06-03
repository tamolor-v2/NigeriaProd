from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['j.adeleke@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_failure': ['j.adeleke@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='insert_ydfs_accounts',
    default_args=args,
    schedule_interval='0 7 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

ydfs_accounts = BashOperator(
     task_id='insert_ydfs_accounts' ,
     bash_command='bash /nas/share05/scripts/ydfs/ydfs_accounts.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

ydfs_accounts