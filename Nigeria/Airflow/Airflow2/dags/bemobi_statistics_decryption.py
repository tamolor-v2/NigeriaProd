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
    'start_date': datetime(2022,10,23), #airflow.utils.dates.days_ago(1),
    'email': ['m.nabeel@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='bemobi_statistics_decryption',
    default_args=args,
    schedule_interval='0 5 * * * ',
    catchup=False,
    concurrency=3,
)

bemobi_statistics_decryption = BashOperator(
     task_id='bemobi_statistics_decryption' ,
     bash_command='bash /nas/share05/tools/BEMOBI_DECRYPTION/decr_cron_BEMOBI_STATISTICS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)