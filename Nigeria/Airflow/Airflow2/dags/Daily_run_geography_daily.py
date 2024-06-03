from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import os

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'catchup':False,
    'depends_on_past':False,
    'start_date': datetime(2019, 8, 21),
}

dag = DAG(
    dag_id='Daily_run_geography',
    default_args=args,
    #schedule_interval='0 8,10,12 * * *',
    schedule_interval=None,
    concurrency=1,
    catchup=True,
    max_active_runs=1
)

t1 = BashOperator(
     task_id='run_geography' ,
     bash_command='bash /nas/share05/scripts/geography/run_geography_daily.sh `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag,
     run_as_user = 'daasuser',
)
    
t2 = BashOperator(
     task_id='log' ,
     bash_command='tail -30 /nas/share05/scripts/log/geography_daily.out   ',
     dag=dag,
     run_as_user = 'daasuser',
)

t1 >> t2
