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
    dag_id='Daily_Reports_c',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

WBS_Summary = BashOperator(
     task_id='WBS_Summary' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p wbs',
     dag=dag,
     run_as_user = 'daasuser',
)

hyconnect_inc_usage_sumd = BashOperator(
     task_id='hyconnect_inc_usage_sumd' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HYCONNECT -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)

payment_summary = BashOperator(
     task_id='payment_summary' ,
     bash_command='perl /nas/share05/ops/scripts/daily_payment_summary.pl',
     dag=dag,
     run_as_user = 'daasuser',
)


roaming_sumd = BashOperator(
     task_id='roaming_sumd' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p ROAMING_SUMD -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)

prepaid_activation = BashOperator(
     task_id='prepaid_activation' ,
     bash_command='perl /nas/share05/ops/daily/prepaid_activation.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)


#dola = BashOperator(
#     task_id='dola' ,
#     bash_command='perl /nas/share05/ops/monthly/dola_id0083.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
#     dag=dag,
#)

#D2C_REPORT = BashOperator(
#     task_id='D2C_REPORT' ,
#     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p D2C_REPORT',
#     run_as_user = 'daasuser',
#     dag=dag,
#)

#INCOMING_TRAFFIC = BashOperator(
#     task_id='INCOMING_TRAFFIC' ,
#     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p INCOMING_TRAFFIC',
#     run_as_user = 'daasuser',
#     dag=dag,
#)

#REP_INCOMING_TRAFFIC_MON = BashOperator(
#     task_id='REP_INCOMING_TRAFFIC_MON' ,
#     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p REP_INCOMING_TRAFFIC_MON',
#     run_as_user = 'daasuser',
#     dag=dag,
#)


TRENDING_ON_OFF_VOICE = BashOperator(
     task_id='trending_on_off_voice' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p TRENDING_ON_OFF_VOICE -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)

SME_DATA_SHARE = BashOperator(
     task_id='SME_DATA_SHARE' ,
     bash_command='/nas/share05/ops/mtnops/daily_reports.py -p SME_DATA_SHARE',
     run_as_user = 'daasuser',
     dag=dag, 
)

REV_PER_BTS_SUMD = BashOperator(
     task_id='REV_PER_BTS_SUMD' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p REV_PER_BTS_SUMD -l 3',
     run_as_user = 'daasuser',
     dag=dag,
)


PREPAID_VAS_USSD = BashOperator(
     task_id='PREPAID_VAS_USSD' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p PREPAID_VAS_USSD -l 4',
     run_as_user = 'daasuser',
     dag=dag,

)


PREPAID_ACT_DEALERS = BashOperator(
     task_id='PREPAID_ACT_DEALERS' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p PREPAID_ACT_DEALERS -l 3',
     run_as_user = 'daasuser',
     dag=dag,

)


DAILY_VOUCHER_REPORT_PRD = BashOperator(
     task_id='DAILY_VOUCHER_REPORT_PRD' ,
     bash_command='perl /nas/share05/ops/mtnops/daily_voucher_report_prd.pl `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser',

)
 
WBS_Summary >> DAILY_VOUCHER_REPORT_PRD >> hyconnect_inc_usage_sumd >> roaming_sumd >> payment_summary >> prepaid_activation >>  TRENDING_ON_OFF_VOICE >> SME_DATA_SHARE >> REV_PER_BTS_SUMD >> PREPAID_VAS_USSD >> PREPAID_ACT_DEALERS
