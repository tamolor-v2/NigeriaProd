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
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='ncc_900k_analysis',
    default_args=args,
    schedule_interval='0 5,10,14 * * *',
    concurrency=1,
    catchup=False,
    max_active_runs=1
)

t1 = BashOperator(
     task_id='fetch_registration' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/spool_agility_simreg.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser'
)

t2 = BashOperator(
     task_id='Ncc_900k_Dashboard' ,
     bash_command='bash /nas/share05/ops/mtnops/optimus_prime.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

t3 = BashOperator(
     task_id='Ncc_63K_Dashboard' ,
     bash_command='bash /nas/share05/ops/mtnops/check_63kbase_barr.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)
    
t1 >> t2 >> t3
