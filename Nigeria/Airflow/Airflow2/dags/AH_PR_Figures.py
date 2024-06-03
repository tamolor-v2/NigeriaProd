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
    dag_id='AH_PR_Figures',
    default_args=args,
    schedule_interval='05 7 * * *',
    concurrency=1,
    catchup=False,
    max_active_runs=1

)

AH_PR_Figures = BashOperator(
     task_id='AH_PR_Figures' ,
     bash_command='bash /nas/share05/ops/mtnops/ah_mthend_PR.sh `date --date="-1 days" +%Y%m%d`	'  ,
     dag=dag,
     run_as_user = 'daasuser'
)


AH_PR_Figures
