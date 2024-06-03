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
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['support@ligadata.com','h.malkawi@ligadata.com'],
    'email_on_failure': ['support@ligadata.com','h.malkawi@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='lz_file_count',
    default_args=args,
#    schedule_interval='0 * * * *',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

hourly_lz_file_count = BashOperator(
     task_id='hourly_lz_file_count' ,
     bash_command='/mnt/beegfs_bsl/tools/LZ_File_Count.py',
     dag=dag,
     run_as_user = 'daasuser'
)

hourly_lz_file_count
