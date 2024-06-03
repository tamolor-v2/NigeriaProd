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
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Month-End-REPORTS_1',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

Available_voucher = BashOperator(
     task_id='Available_voucher' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p available_voucher',
     dag=dag,
     run_as_user='daasuser'
)

AWUF4U = BashOperator(
     task_id='AWUF4U' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p AWUF4U',
     dag=dag,
     run_as_user='daasuser'
)

awuf_4_you = BashOperator(
     task_id='awuf_4_you' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p awuf_4_you',
     dag=dag,
     run_as_user='daasuser'
)

bill_summary = BashOperator(
     task_id='bill_summary' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p bill_summary',
     dag=dag,
     run_as_user='daasuser'
)

blacklisted_voucher = BashOperator(
     task_id='blacklisted_voucher' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p blacklisted_voucher',
     dag=dag,
     run_as_user='daasuser'
)

call_reason_dashboard = BashOperator(
     task_id='call_reason_dashboard' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p call_reason_dashboard -l 5',
     dag=dag,
     run_as_user='daasuser'
)

#Cardload = BashOperator(
#     task_id='Cardload' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p cardload -l 3',
#     dag=dag,
#)

CRBT_Download = BashOperator(
     task_id='CRBT-Download' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p CRBT_DOWNLOAD',
     dag=dag,
     run_as_user='daasuser'
)

CRBT_MUSICBOX = BashOperator(
     task_id='CRBT_MUSICBOX' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p CRBT_MUSICBOX',
     dag=dag,
     run_as_user='daasuser'
)

Crbt_Subscription = BashOperator(
     task_id='Crbt_Subscription' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Crbt_Subscription',
     dag=dag,
     run_as_user='daasuser'
)

Customer_Credit_Info = BashOperator(
     task_id='Customer_Credit_Info' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Customer_Credit_Info -l 5',
     dag=dag,
     run_as_user='daasuser'
)

Daily_LTE_Recon = BashOperator(
     task_id='Daily_LTE_Recon' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Daily_LTE_Recon -l 5',
     dag=dag,
     run_as_user='daasuser'
)

DATA_BUNDLE = BashOperator(
     task_id='DATA_BUNDLE' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p DATA_BUNDLE',
     dag=dag,
     run_as_user='daasuser'
)

Available_voucher >>  DATA_BUNDLE >>   bill_summary >> AWUF4U >> awuf_4_you >> Daily_LTE_Recon >> Customer_Credit_Info >> Crbt_Subscription >> CRBT_MUSICBOX >> CRBT_Download >> call_reason_dashboard >> blacklisted_voucher
