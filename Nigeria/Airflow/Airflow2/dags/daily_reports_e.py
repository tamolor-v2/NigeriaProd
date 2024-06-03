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
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
dag = DAG(
    dag_id='Daily_Reports_e',
    default_args=args,
    schedule_interval='0 5 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

SMS_TRACKING_REPORT = BashOperator(
     task_id='sms_tracking_report' ,
     bash_command='/nas/share05/ops/mtnops/sms_tracking_report.py',
     dag=dag,
     run_as_user = 'daasuser'
)

SMS_TRACKIN = BashOperator(
     task_id='sms_trackin' ,
     bash_command='/nas/share05/ops/mtnops/sms_trend_apr_new.py',
     dag=dag,
     run_as_user = 'daasuser'
)

repair = BashOperator(
     task_id='repair' ,
     bash_command='bash /nas/share05/ops/mtnops/repair_table.sh	',
     dag=dag,
     run_as_user = 'daasuser'
)

#card_load = BashOperator(
#     task_id='card_load' ,
#     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p cardload -l 3',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

XTRABITE_SUMMARY = BashOperator(
     task_id='XTRABITE_SUMMARY' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p XTRABITE_SUMMARY',
     dag=dag,
     run_as_user = 'daasuser'
)

ERS_TRANSACTIONS = BashOperator(
     task_id='ERS_TRANSACTIONS' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p ERS_TRANSACTIONS',
     dag=dag,
     run_as_user = 'daasuser'
)

PORT_OUT = BashOperator(
     task_id='port_out' ,
     bash_command='bash /nas/share05/ops/daily/port_out.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

CIS_CATALOGUE = BashOperator(
     task_id='cis_catalogue' ,
     bash_command='bash /nas/share05/ops/daily/cis_catalogue.sh ',
     dag=dag,
     run_as_user = 'daasuser',
)

#DOLA = BashOperator(
#     task_id='dola' ,
#     bash_command='perl /nas/share05/ops/monthly/dola_id0083.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
#     dag=dag,
#     run_as_user = 'daasuser',
#)


DAILY_MNP_TRANSACTIONS = BashOperator(
     task_id='daily_mnp_transactions' ,
     bash_command='/nas/share05/ops/mtnops/daily_reports.py -p DAILY_MNP_TRANSACTIONS -l 3',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser',
)

M2M_OFFERS = BashOperator(
     task_id='M2M_OFFERS' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p M2M_OFFERS',
     dag=dag,
     run_as_user = 'daasuser',
)


AGL_GDS_PROVIDER_CHILD = BashOperator(
     task_id='AGL_GDS_PROVIDER_CHILD' ,
     bash_command='/nas/share05/ops/mtnops/agl_gds_provider_child.py -p `date --date="-1 days" +%Y%m%d` ',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser',
)

#MSO_DATA_INFO = BashOperator(
#     task_id='MSO_DATA_INFO' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/MSO_DATA_INFO.py `date --date="-1 days" +%Y%m%d`',
#     depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser',
#
#)


SMS_TRACKING_REPORT >> SMS_TRACKIN >> DAILY_MNP_TRANSACTIONS >> PORT_OUT >> repair >> XTRABITE_SUMMARY >> ERS_TRANSACTIONS >> M2M_OFFERS >> CIS_CATALOGUE >> AGL_GDS_PROVIDER_CHILD
##>> USSD_Summary >> MSO_DATA_INFO
