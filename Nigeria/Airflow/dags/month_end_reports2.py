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
    dag_id='Month-End-REPORTS_2',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

Device_Dagc = BashOperator(
     task_id='Device_Dagc' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p DEVICE_GAGC',
     dag=dag,
     run_as_user = 'daasuser'
)

DYA_VISAFONE_Report = BashOperator(
     task_id='DYA_VISAFONE_Report' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p DYA_VISAFONE_Report',
     dag=dag,
     run_as_user = 'daasuser'
)

AirTimeOnDemand = BashOperator(
     task_id='AirTime-On-Demand' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p airtime_on_demand',
     dag=dag,
     run_as_user = 'daasuser'
)

Epostpaid_Blackberry = BashOperator(
     task_id='Epostpaid_Blackberry' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Epostpaid_Blackberry',
     dag=dag,
     run_as_user = 'daasuser'
)

Extra_data = BashOperator(
     task_id='Extra_data' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Extra_data',
     dag=dag,
     run_as_user = 'daasuser'
)

Extratime_commission = BashOperator(
     task_id='Extratime_commission' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p EXTRATIME_COMMISION',
     dag=dag,
     run_as_user = 'daasuser'
)



Extratime_report = BashOperator(
     task_id='Extratime_report' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Extratime_report',
     dag=dag,
     run_as_user = 'daasuser'
)

FCMB_sponsored = BashOperator(
     task_id='FCMB_sponsored' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p FCMB_sponsored',
     dag=dag,
     run_as_user = 'daasuser'
)

GPRS_REPORT = BashOperator(
     task_id='GPRS_REPORT' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p GPRS_REPORT',
     dag=dag,
     run_as_user = 'daasuser'
)

Gprs_Summary = BashOperator(
     task_id='Gprs-Summary' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p GprsSummary',
     dag=dag,
     run_as_user = 'daasuser'
)

Hajj_Roaming = BashOperator(
     task_id='Hajj_Roaming' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Hajj_Roaming',
     dag=dag,
     run_as_user = 'daasuser'
)

EPOSTPAID_ROAMING_REP = BashOperator(
     task_id='EPOSTPAID_ROAMING_REP' ,
     bash_command='perl /nas/share05/ops/bin/epostpaid_roaming_rep.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` ',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SDP_DA_BAL_SC = BashOperator(
     task_id='sdp_da_bal_sc' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SDP_DA_BAL_SC ',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

MOD_FASTLINK_REPORT = BashOperator(
     task_id='mod_fastlink_report' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MOD_FASTLINK_REPORT ',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

#tpc_package1 = BashOperator(
     #task_id='tpc_package1' ,
     #bash_command='perl /nas/share05/ops/cleanup/TPC_PACKAGE1_DeviceActivation.pl TPC_PACKAGE1 `date --date="-1 days" +%Y%m%d`',
     #dag=dag,
     #run_as_user = 'daasuser'
#)

GPRS_REPORT >> FCMB_sponsored >> Extratime_report >> Extratime_commission >> Extra_data >> AirTimeOnDemand >> DYA_VISAFONE_Report >>  Device_Dagc >> Gprs_Summary >> Hajj_Roaming >> MOD_FASTLINK_REPORT >> SDP_DA_BAL_SC >> EPOSTPAID_ROAMING_REP >> Epostpaid_Blackberry
