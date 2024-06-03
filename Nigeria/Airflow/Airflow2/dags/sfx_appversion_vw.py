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
    'catchup':False,
}

dag = DAG(
    dag_id='SFX_APPVERSION_VW',
    default_args=args,
    schedule_interval='*/20 * * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

SFX_APPVERSION_VW = BashOperator(
     task_id='SFX_APPVERSION_VW' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py  -l 2 -p SFX_APPVERSION_VW',
     dag=dag,
     run_as_user = 'daasuser',
)

SFX_APPVERSION_VW
