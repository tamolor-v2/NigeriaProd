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
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='ONDEMAND_TIMEOUT_15',
    default_args=args,
    schedule_interval='5 22 * * *',
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)

ONEDEMAND_TIMEOUT = BashOperator(
     task_id='ondemand_timeout' ,
     bash_command='ssh 10.1.197.149 bash /nas/share05/ops/mtnops/qrios_timeout.sh `date  +%Y%m%d` 15  ',
     run_as_user = 'daasuser',
     dag=dag,
)

ONEDEMAND_TIMEOUT
