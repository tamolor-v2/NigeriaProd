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
from collections import defaultdict
from os import popen
import os
import re


today =  datetime.today()
dateRun = datetime.today() + timedelta(days=int(-1))
#dateRunStr = dateRun.strftime('%Y%m%d')
todayStr = today.strftime('%Y%m%d%H%m%s')

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['m.nabeel@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','support@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='CVM20_DailyRuns',
    default_args=args,
    schedule_interval='0 8 * * * ',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)

def getDate(date_str):
    date = datetime.strptime(date_str[0:10], '%Y-%m-%d')
    currentdate=(date - timedelta(0)).strftime('%Y%m%d')
    return currentdate

getDateNode = PythonOperator(
    task_id = 'Get_Date',
    priority_weight = 10,
    python_callable = getDate,
    op_args=[popen('echo {{ execution_date }};').read()],
    dag=dag
)

dateRunStr="{{ task_instance.xcom_pull(task_ids='Get_Date') }}"

cvm20_distributor_activations = BashOperator(
     task_id='cvm20_distributor_activations' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_DISTRIBUTOR_ACTIVATIONS_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_refill_info = BashOperator(
     task_id='cvm20_refill_info' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REFILL_INFO_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_bundle_transaction = BashOperator(
     task_id='cvm20_bundle_transaction' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_BUNDLE_TRANSACTION_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_bundle_transaction_awuf4u = BashOperator(
     task_id='cvm20_bundle_transaction_awuf4u' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_BUNDLE_TRANSACTION_AWUF4U_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_ayoba = BashOperator(
     task_id='cvm20_ayoba' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_AYOBA_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_momo_daily_kpis = BashOperator(
     task_id='cvm20_momo_daily_kpis' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_MOMO_DAILY_KPIS_base.sh {0} '.format(dateRunStr,todayStr),
     dag=dag,
     run_as_user = 'daasuser'
)

