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
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d') 

dag = DAG(
    dag_id='AYOBA_VALIDATION',
    default_args=args,
    schedule_interval='0 22 * * *',
    catchup=False,
    concurrency=4,
    max_active_runs=4
)


job_1 = BashOperator(
     task_id='Ayoba_Validation' ,
     bash_command='ssh edge01001 python3.6 /nas/share05/ops/mtnops/ayoba_validation.py -l 1',
     dag=dag,
     run_as_user = 'daasuser'
)


job_1 
