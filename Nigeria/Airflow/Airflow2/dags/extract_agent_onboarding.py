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

today = datetime.today()
dateRun = datetime.today() + timedelta(days=int(-1))
dateRunStr = dateRun.strftime('%Y%m%d')

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','t.olorunfemi@ligadata.com','s.iyaju@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

dag = DAG(
    dag_id='AGENT_ONBOARDING',
    default_args=args,
    schedule_interval='30 7 * * *',
    description='DO NOT TURN OFF',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

AGENT_ONBOARDING = BashOperator(
    task_id='AGENT_ONBOARDING' ,
    bash_command='bash /nas/share05/tools/ExtractTools/AGENT_ONBOARDING/extract_AGENT_ONBOARDING.sh ',
    run_as_user='daasuser',
    dag=dag,
)