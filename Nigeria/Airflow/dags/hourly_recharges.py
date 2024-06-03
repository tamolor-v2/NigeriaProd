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
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='HOURLY',
    default_args=args,
    schedule_interval='0 * * * * ',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

HOURLY_RECHARGES = BashOperator(
     task_id='HOURLY_RECHARGES' ,
     depends_on_past=True,
     bash_command=' bash /nas/share05/ops/scripts/hourly_recharges.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

PARTNER_BALANCES = BashOperator(
     task_id='PARTNER_BALANCES' ,
     depends_on_past=True,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p PARTNER_BALANCES -l 1',
     dag=dag,
     run_as_user = 'daasuser'
)

DAAS_DAILY_STAT = BashOperator(
     task_id='DAAS_DAILY_STAT' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p DAAS_DAILY_STAT ',
     dag=dag,
     run_as_user = 'daasuser'
)

HOURLY_FEED_REPORT = BashOperator(
     task_id='HOURLY_FEED_REPORT' ,
     bash_command='ssh edge01001 python3.6 /nas/share05/ops/mtnops/monitoring.py -p HOURLY -l 1 ',
     run_as_user = 'daasuser',
     dag=dag,
)


SMS_TREND = BashOperator(
     task_id='SMS_TREND' ,
     #bash_command='/nas/share05/ops/mtnops/sms_trend_apr.py',
     bash_command='echo 1',
     dag=dag,
     run_as_user = 'daasuser'
)

HOURLY_RECHARGES >> PARTNER_BALANCES >> DAAS_DAILY_STAT >> HOURLY_FEED_REPORT >>  SMS_TREND
