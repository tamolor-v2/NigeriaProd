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
    dag_id='CS18_Daily_Reports_Batch_1',
    default_args=args,
    schedule_interval='* 10-18 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

DAILY_CDR_SUMMARY = BashOperator(
     task_id='Daily_CDR_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p DAILY_CDR_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
DAILY_CDR_SUMMARY

chatbox = BashOperator(
     task_id='Chat_Box' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p chatbox -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
chatbox

EVD_BY_PARTNER = BashOperator(
     task_id='EVD_Recharges_by_Parthner' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p EVD_BY_PARTNER -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
EVD_BY_PARTNER

EXBYTE_EXTIME = BashOperator(
     task_id='Extra-Time_Extra-Byte' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p EXBYTE_EXTIME -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
EXBYTE_EXTIME

REFILL_TREND = BashOperator(
     task_id='Refill_Trend_Analysis' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p REFILL_TREND -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
REFILL_TREND

MYMTNAPPS = BashOperator(
     task_id='MyMTN_Apps' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p MYMTNAPPS -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
MYMTNAPPS

SHORT_CODE = BashOperator(
     task_id='Short_Codes_Report' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p SHORT_CODE -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
SHORT_CODE

MOD_TRANSACTION = BashOperator(
     task_id='MOD_Transactions' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p MOD_TRANSACTION -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
MOD_TRANSACTION

USSD_BY_LOC = BashOperator(
     task_id='USSD_By_Location' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p USSD_BY_LOC -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
USSD_BY_LOC

FREE_DATA = BashOperator(
     task_id='Free_Data_Report' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p FREE_DATA -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
FREE_DATA

CIS_CDR = BashOperator(
     task_id='Daily_CIS_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p CIS_CDR -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)
CIS_CDR

PAY_SUMMARIES = BashOperator(
     task_id='Payment_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p PAY_SUMMARIES -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
PAY_SUMMARIES

CASH_VEND = BashOperator(
     task_id='Cash_Vending' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p CASH_VEND -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
CASH_VEND

TRAVELLER_PLAN = BashOperator(
     task_id='Traveller_Plan' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p TRAVELLER_PLAN -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
TRAVELLER_PLAN

BIZCONNECT = BashOperator(
     task_id='BizPlus_Biz_Connect' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p BIZCONNECT -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
BIZCONNECT

CASH_VEND >> DAILY_CDR_SUMMARY >> TRAVELLER_PLAN >> chatbox >> BIZCONNECT >> EVD_BY_PARTNER >> EXBYTE_EXTIME >> REFILL_TREND >> MYMTNAPPS >> SHORT_CODE >> MOD_TRANSACTION >> USSD_BY_LOC >> FREE_DATA >> CIS_CDR >> PAY_SUMMARIES
