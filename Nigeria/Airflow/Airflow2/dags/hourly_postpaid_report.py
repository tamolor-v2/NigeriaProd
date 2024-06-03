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
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
    'depends_on_past':False,
}

dag = DAG(
    dag_id='HOURLY_POSTPAID_RPT',
    default_args=args,
    schedule_interval=' 0 4,12 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

t1 = BashOperator(
     task_id='HOURLY_POSTPAID_RPT' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HOURLY_POSTPAID_RPT -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)

t1
