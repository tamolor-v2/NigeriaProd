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
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='AYOBA_RESOLVED_EOD',
    default_args=args,
    schedule_interval='0 21 * * *',
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)
Ayoba_Resolved_EOD = BashOperator(
     task_id='ayoba_resolved_eod' ,
     bash_command='ssh datanode01002 bash /nas/share05/FlareProd/Run/Ayoba/AyobaResolved/Ayoba_Resolved_EOD.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)

Ayoba_Resolved_EOD 

