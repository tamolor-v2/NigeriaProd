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
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
date_param_1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
 
dag = DAG(
    dag_id='Month-End-REPORT_test_1',
    default_args=args,
#    schedule_interval='0 6 * * *',
    schedule_interval=None,
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

Available_voucher = BashOperator(
     task_id='Available_voucher' ,
     bash_command='echo {0} {1}'.format(date_param,date_param_1),
     dag=dag,
     run_as_user='daasuser'
)
Available_voucher
