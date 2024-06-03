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
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='WBS_RECON_DAILY',
    default_args=args,
    schedule_interval='0 7,12,18 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

WBS_RECON_DAILY = BashOperator(
     task_id='WBS_RECON_DAILY' ,
     bash_command='ssh datanode01038 python3.6 /nas/share05/ops/mtnops/data_ingestion.py  -p wbs_recon_daily -l 3 -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser',
)

WBS_RECON_DAILY
