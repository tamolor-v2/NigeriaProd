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
    dag_id='Month-End-REPORTS_3',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

handset_device = BashOperator(
     task_id='handset_device' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p handset_device',
     dag=dag,
     run_as_user = 'daasuser'
)


Helloworld_Roaming = BashOperator(
     task_id='Helloworld_Roaming' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p HELLOWORLD_ROAMING',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

hsdp_blackberry = BashOperator(
     task_id='hsdp_blackberry' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p hsdp_blackberry',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

hynet_usage = BashOperator(
     task_id='hynet_usage' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p hynet_usage',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

idb_report = BashOperator(
     task_id='idb_report' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p IBD_REPORT',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

imi_mobile = BashOperator(
     task_id='imi_mobile' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p imi_mobile',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

JUST4U = BashOperator(
     task_id='JUST4U' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p JUST4U',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

#MEDIATION_METRICS = BashOperator(
#     task_id='MEDIATION_METRICS' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p MEDIATION_METRICS -l 3',
#     depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser'
#)

MON_AOD = BashOperator(
     task_id='MON_AOD' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MON_AOD',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

EPOSTPAID_RECHARGE = BashOperator(
     task_id='EPOSTPAID_RECHARGE_REPORT' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p EPOSTPAID_RECHARGE',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

VAS_REPORTS = BashOperator(
     task_id='VAS_REPORTS' ,
     bash_command='perl /nas/share05/ops/bin/vas_reports.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` ',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

XTRATIME_CLOSING_SDP_REPORT = BashOperator(
     task_id='XTRATIME_CLOSING_SDP_REPORT' ,
     bash_command='perl /nas/share05/ops/monthly/xtratime_closing_sdp_id0232.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` ',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

GPRS_SUMM_REPORT = BashOperator(
     task_id='GPRS_SUMM_REPORT' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p GPRS_SUMM_REPORT',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

VTU_DATA_SUBSCRIPTION = BashOperator(
     task_id='VTU_DATA_SUBSCRIPTION' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p vtu_data_subs',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


VTU_DATA_SUBSCRIPTION >> hynet_usage >> idb_report >> imi_mobile >> JUST4U >> MON_AOD >> EPOSTPAID_RECHARGE >> VAS_REPORTS >> XTRATIME_CLOSING_SDP_REPORT >> GPRS_SUMM_REPORT >> hsdp_blackberry >> handset_device >>  Helloworld_Roaming
