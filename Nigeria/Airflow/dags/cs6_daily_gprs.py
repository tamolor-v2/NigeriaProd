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
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='CS6_Usages_GPRS',
    default_args=args,
#    schedule_interval='* 1,3,5,6,9,11,17,19,21,23 * * *',
    schedule_interval=None,
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

usages = BashOperator(
     task_id='Usages' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs6_usage_summary.py -p gprs -l 4',
     dag=dag,
     run_as_user = 'daasuser',
)

usages
