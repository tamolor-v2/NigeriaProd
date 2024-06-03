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
    dag_id='DATA_REVENUE_RERUN',
    default_args=args,
    schedule_interval='20 5 * * *',
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)
DATA_REVENUE_RERUN = BashOperator(
     task_id='data_revenue_rerun' ,
     bash_command='/nas/share05/ops/mtnops/data_revenue.py -l 5 -s `date --date="-1 days" +%Y-%m-%d`',
     run_as_user = 'daasuser',
     dag=dag,
)

DATA_REVENUE_RERUN 
