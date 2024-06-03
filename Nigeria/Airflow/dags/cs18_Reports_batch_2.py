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
    dag_id='CS18_Daily_Reports_Batch_2',
    default_args=args,
    schedule_interval='* 18-22 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

WBS_SUMMARY = BashOperator(
     task_id='WBS_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p chatbox -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
WBS_SUMMARY

MARKET_SHARE = BashOperator(
     task_id='Market_Share' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p MARKET_SHARE -s `date --date="-1 days" +%Y-%m-%d` -l 2',
     dag=dag,
     run_as_user = 'daasuser',
)
MARKET_SHARE

SPONSORED_DATA = BashOperator(
     task_id='Sponsored_Data' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p SPONSORED_DATA -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
SPONSORED_DATA

RECYCLE_NUMBER = BashOperator(
     task_id='Recycled_Numbers' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p RECYCLE_NUMBER -s `date --date="-1 days" +%Y-%m-%d` -l 2',
     dag=dag,
     run_as_user = 'daasuser',
)
RECYCLE_NUMBER


WBS_SUMMARY >> SPONSORED_DATA >> MARKET_SHARE >> RECYCLE_NUMBER
