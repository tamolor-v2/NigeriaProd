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

today =  datetime.today()
dateRun = datetime.today() + timedelta(days=int(-1))
dateRunStr = dateRun.strftime('%Y%m%d')
todayStr = today.strftime('%Y%m%d%H%m%s')

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['m.nabeel@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Credit_Score',
    default_args=args,
    schedule_interval='0 7 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

credit_score = BashOperator(
     task_id='credit_score' ,
     bash_command='bash /nas/share05/tools/CREDIT_SCORE/Insert_CREDIT_SCORE_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

