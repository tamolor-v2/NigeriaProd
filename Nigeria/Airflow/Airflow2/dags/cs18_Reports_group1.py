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
    dag_id='CS18_Daily_Usage_Cluster_Group_1',
    default_args=args,
    schedule_interval='* 10,18 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

DAILY_CDR_SUMMARY = BashOperator(
     task_id='Daily_CDR_Summary' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p DAILY_CDR_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

chatbox = BashOperator(
     task_id='Chat_Box' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p chatbox -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

EVD_BY_PARTNER = BashOperator(
     task_id='EVD_Recharges_by_Parthner' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p EVD_BY_PARTNER -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

EXBYTE_EXTIME = BashOperator(
     task_id='Extra-Time_Extra-Byte' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p EXBYTE_EXTIME -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

REFILL_TREND = BashOperator(
     task_id='Refill_Trend_Analysis' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p REFILL_TREND -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

MYMTNAPPS = BashOperator(
     task_id='MyMTN_Apps' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p MYMTNAPPS -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

SHORT_CODE = BashOperator(
     task_id='Short_Codes_Report' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p SHORT_CODE -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

MOD_TRANSACTION = BashOperator(
     task_id='MOD_Transactions' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p MOD_TRANSACTION -l 22 -s `date --date="-1 days" +%Y-%m-%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)

CS18_MOU = BashOperator(
     task_id='CS18_MOU' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p CS18_MOU -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

M2M_OFFERS = BashOperator(
     task_id='M2M_OFFERS' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p M2M_OFFERS -s `date --date="-1 days" +%Y-%m-%d` -l 7',
     dag=dag,
     run_as_user = 'daasuser',
)

DAILY_CDR_SUMMARY >> chatbox >> EVD_BY_PARTNER >> EXBYTE_EXTIME >> REFILL_TREND >> MYMTNAPPS >> SHORT_CODE >> MOD_TRANSACTION >> CS18_MOU >> M2M_OFFERS
