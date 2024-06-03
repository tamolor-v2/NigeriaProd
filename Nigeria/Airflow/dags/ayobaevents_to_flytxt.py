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
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d') 

dag = DAG(
    dag_id='AYOBAEVENTS_TO_FLYTXT',
    default_args=args,
    schedule_interval='2 0-23 * * *',
    catchup=False,
    concurrency=4,
    max_active_runs=4
)


ayobaevents_to_flytxt = BashOperator(
     task_id='ayobaevents_to_flytxt' ,
     bash_command='bash /nas/share05/tools/DBExtrct/automated_scripts/AyobaEvents_to_Flytxt.sh	',
     dag=dag,
     run_as_user = 'daasuser'
)

ayobaevents_to_flytxt 

