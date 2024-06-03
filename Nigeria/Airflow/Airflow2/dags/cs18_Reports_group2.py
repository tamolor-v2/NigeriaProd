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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='CS18_Daily_Usage_Cluster_Group_2',
    default_args=args,
    schedule_interval='* 10,18 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

VOICE_SMS = BashOperator(
     task_id='Voice_SMS_Revenue' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p VOICE_SMS_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

PIVOT_SUMMARY = BashOperator(
     task_id='Daily_Usage_Summary_by_Service_Class' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/platform_revenue.py -p PIVOT_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

USSD_BY_LOC = BashOperator(
     task_id='USSD_By_Location' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p USSD_BY_LOC -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

FREE_DATA = BashOperator(
     task_id='Free_Data_Report' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p FREE_DATA -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

CIS_CDR = BashOperator(
     task_id='Daily_CIS_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p CIS_CDR -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

PAY_SUMMARIES = BashOperator(
     task_id='Payment_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p PAY_SUMMARIES -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

CASH_VEND = BashOperator(
     task_id='Cash_Vending' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p CASH_VEND -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

TRAVELLER_PLAN = BashOperator(
     task_id='Traveller_Plan' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p TRAVELLER_PLAN -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

BIZCONNECT = BashOperator(
     task_id='BizPlus_Biz_Connect' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p BIZCONNECT -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

PIVOT_SUMMARY >> VOICE_SMS >> USSD_BY_LOC >> FREE_DATA >> CIS_CDR >> PAY_SUMMARIES >> CASH_VEND >> TRAVELLER_PLAN >> BIZCONNECT
