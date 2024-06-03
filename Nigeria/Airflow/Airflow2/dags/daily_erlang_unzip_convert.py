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

today = datetime.today()
dateRun = datetime.today() + timedelta(days=int(-1))
dateRunStr = dateRun.strftime('%Y%m%d')

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

dag = DAG(
    dag_id='DAILY_ERLANG_UNZIP_CONVERT',
    default_args=args,
    schedule_interval='30 11,13,15 * * *',
    description='DO NOT TURN OFF',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

DAILY_ERLANG_UNZIP_CONVERT = BashOperator(
    task_id='DAILY_ERLANG_UNZIP_CONVERT' ,
    bash_command='bash /nas/share05/tools/DAILY_ERLANG/Extract_Convert/unzip_and_convert.sh ',
    run_as_user='daasuser',
    dag=dag,
)