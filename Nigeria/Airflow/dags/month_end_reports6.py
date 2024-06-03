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
    'email': ['o.olanipekun@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Month-End-REPORTS_6',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

EPOSTPAID_VTU = BashOperator(
     task_id='epostpaid_vtu' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p epostpaid_vtu',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

DYA_VISAFONE_REPORT = BashOperator(
     task_id='DYA_VISAFONE_Report' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p DYA_VISAFONE_Report',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

DAILY_RECHARGES_VTU = BashOperator(
     task_id='daily_recharges_vtu' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p DAILY_RECHARGES_VTU',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

VISAFONE_ADJ_REPORT = BashOperator(
     task_id='visafone_adj_report' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p VISAFONE_ADJ_REPORT',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

STOCK_IN_CHANNEL_AGEING = BashOperator(
     task_id='stock_in_channel_ageing' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p STOCK_IN_CHANNEL_AGEING',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

PREPAID_ROAMING_REV = BashOperator(
     task_id='PREPAID_ROAMING_REV' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p PREPAID_ROAMING_REV',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SMARTAPP_AND_DATARESET = BashOperator(
     task_id='SMARTAPP_AND_DATARESET' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SMARTAPP_AND_DATARESET',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

ADSL_OFFERS = BashOperator(
     task_id='ADSL_OFFERS' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p ADSL_OFFERS',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

EPOSTPAID_DATA = BashOperator(
     task_id='EPOSTPAID_DATA' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p EPOSTPAID_DATA',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

EXTRA_RENEWAL = BashOperator(
     task_id='EXTRA_RENEWAL' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p EXTRA_RENEWAL',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

EXBYTE_EXTIME_COMM = BashOperator(
     task_id='EXBYTE_EXTIME_COMM' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p EXBYTE_EXTIME_COMM',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

EXTRA_COMM_REPORT = BashOperator(
     task_id='EXTRA_COMM_REPORT' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p EXTRA_COMM_REPORT',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SHARE_AND_SELL = BashOperator(
     task_id='SHARE_AND_SELL' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SHARE_AND_SELL',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SMS_REVENUE_MA = BashOperator(
     task_id='SMS_REVENUE_MA' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SMS_REVENUE_MA',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SMS_REVENUE_DA = BashOperator(
     task_id='SMS_REVENUE_DA' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SMS_REVENUE_DA',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


EPOSTPAID_VTU >> DYA_VISAFONE_REPORT >> DAILY_RECHARGES_VTU >> VISAFONE_ADJ_REPORT >> STOCK_IN_CHANNEL_AGEING >> PREPAID_ROAMING_REV >> SMARTAPP_AND_DATARESET >> ADSL_OFFERS >> EPOSTPAID_DATA >> EXTRA_RENEWAL >> EXBYTE_EXTIME_COMM >> EXTRA_COMM_REPORT >> SHARE_AND_SELL >> SMS_REVENUE_MA >> SMS_REVENUE_DA
