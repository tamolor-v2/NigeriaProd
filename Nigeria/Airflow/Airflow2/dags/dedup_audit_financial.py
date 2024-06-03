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
    'email': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

dag = DAG(
    dag_id='DEDUP_AUD_FIN', 
    default_args=args,
    schedule_interval='59 * * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=1

)
eb_reports = BashOperator(
     task_id='audit' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-7 days" +%Y%m%d`  `date --date="-1 days" +%Y%m%d` AUDIT_LOGS',
     dag=dag,
     run_as_user = 'daasuser'
)

new = BashOperator(
     task_id='financial' ,
     bash_command='python2 /nas/share05/opsScripts/dedupPartitions.py FINANCIAL_LOG `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


