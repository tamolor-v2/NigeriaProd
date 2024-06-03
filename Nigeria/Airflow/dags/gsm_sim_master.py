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
    'depends_on_past': True,
    'email': ['olorunsegun.adeniyi@mtn.com','t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['olorunsegun.adeniyi@mtn.com','t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=60),
    'catchup':False,
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 8),
}

dag = DAG(
    dag_id='GSM_SIM_Master',
    default_args=args,
    schedule_interval='0 5 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

t1 = BashOperator(
     task_id='GSM_MASTER' ,
     bash_command='/nas/share05/ops/mtnops/GSM_SERVICE_MASTER.py `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


t1
