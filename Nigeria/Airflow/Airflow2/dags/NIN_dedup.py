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
    'email': ['h.abdusalam@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['h.abdusalam@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Dedup_NIN_Enrollment',
    default_args=args,
    schedule_interval='0 */2 * * *',
    concurrency=1,
    catchup=False,
    max_active_runs=1

)

Dedup_NIN_Enrollment = BashOperator(
     task_id='Dedup_NIN_Enrollment' ,
     bash_command='/nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f NIN_ENROLLMENT -p m1004 -d $(date +%Y%m%d) -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_NIN_$(date +%Y%m%d%H%M%S).txt'  ,
     dag=dag,
     run_as_user = 'daasuser'
)


Dedup_NIN_Enrollment
