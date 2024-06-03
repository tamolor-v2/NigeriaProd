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
 
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

 
 
dag = DAG(
    dag_id='AYOBA_DEDUP',
    default_args=args,
    schedule_interval='0 7-21/2 * * *',
    description='DO NOT TURN OFF',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)


AYOBA_DEDUP = BashOperator(
     task_id='AYOBA_DEDUP' ,
     bash_command='bash /nas/share05/FlareProd/Run/Ayoba/AyobaResolved/AYOBA_DEDUP.sh  ',
     run_as_user='daasuser',
     dag=dag,
)


AYOBA_DEDUP
