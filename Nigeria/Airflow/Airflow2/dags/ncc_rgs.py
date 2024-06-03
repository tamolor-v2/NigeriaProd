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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

dag = DAG(
    dag_id='NCC_RGS',
    default_args=args,
#    schedule_interval='0 5,17 * * *',
    schedule_interval=None,
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

NCC_BSL = BashOperator(
     task_id='NCC_BSL' ,
     bash_command='perl /nas/share05/ops/daily/ncc_bsl.pl `date --date="-2 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

NCC_EVENT = BashOperator( 
     task_id='NCC_EVENT' ,
     bash_command='perl /nas/share05/ops/daily/ncc_events.pl `date --date="-2 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


NCC_BSL >> NCC_EVENT 
