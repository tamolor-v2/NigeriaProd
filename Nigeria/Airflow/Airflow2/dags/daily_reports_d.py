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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Daily_Reports_d',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

CGI_Validation = BashOperator(
     task_id='CGI_Validation' ,
     bash_command='/nas/share05/ops/mtnops/usage_summary.py -p CGI_VALIDATION -l 5',
     depends_on_past= True,
     dag=dag,
     run_as_user = 'daasuser',
)

#exbyte_extime_comm = BashOperator(
 #    task_id='exbyte_extime_comm' ,
  #   bash_command='perl /nas/share05/ops/daily/exbyte_extime_comm_id50019.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
   #  dag=dag,
    # run_as_user = 'daasuser',
#)

GGSN_Traffic = BashOperator(
     task_id='GGSN_Traffic' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p ggsn',
     dag=dag,
     run_as_user = 'daasuser',
)

MSC_CCN_RECON_ID = BashOperator(
     task_id='msc_ccn_recon_id' ,
     bash_command='/nas/share05/ops/mtnops/daily_reports.py -p MSC_CCN_RECON ',
     dag=dag,
     run_as_user = 'daasuser',
)  

PARTNER_BALANCES = BashOperator(
     task_id='partner_balances' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p PARTNER_BALANCES',
     dag=dag,
     run_as_user = 'daasuser',
)

RATED_VOICE = BashOperator(
     task_id='RATED_VOICE' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_VOICE -l 3',
     dag=dag,
)

RATED_SMS = BashOperator(
     task_id='RATED_SMS' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_SMS -l 3',
     dag=dag,
)

RATED_GPRS = BashOperator(
     task_id='RATED_GPRS' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_GPRS -l 3',
     dag=dag,
)

RATED_ADJ_MA = BashOperator(
     task_id='RATED_ADJ_MA' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_ADJ_MA -l 3',
     dag=dag,
)

RATED_REFILL_MA = BashOperator(
     task_id='RATED_REFILL_MA' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_REFILL_MA -l 3',
     dag=dag,
)

RATED_MSC = BashOperator(
     task_id='RATED_MSC' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_MSC -l 3',
     dag=dag,
)

RATED_GGSN = BashOperator(
     task_id='RATED_GGSN' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_GGSN -l 3',
     dag=dag,
)

RATED_VOICE_DA = BashOperator(
     task_id='RATED_VOICE_DA' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_VOICE_DA -l 3',
     dag=dag,
)

RATED_SMS_DA = BashOperator(
     task_id='RATED_SMS_DA' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_SMS_DA -l 3',
     dag=dag,
)

RATED_B4U_VOICE = BashOperator(
     task_id='RATED_B4U_VOICE' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_B4U_VOICE -l 3',
     dag=dag,    
)

RATED_B4U_GPRS = BashOperator(
     task_id='RATED_B4U_GPRS' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p RATED_B4U_GPRS -l 3',
     dag=dag,
)


FILE_RECON_SUMMARY = BashOperator(
     task_id='FILE_RECON_SUMMARY' ,
     run_as_user = 'daasuser',
     bash_command='/nas/share05/ops/mtnops/daily_reports.py -p FILE_RECON_SUMMARY -l 3',
     dag=dag,
)

#CARDLOAD = BashOperator(
#     task_id='cardload' ,
#     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p cardload -l 3 -s `date --date="-1 days" +%Y-%m-%d`',
#     dag=dag,
#     run_as_user = 'daasuser',
#)

HYNET_REVENUE_SHARE = BashOperator(
     task_id='HYNET_REVENUE_SHARE' ,
     bash_command='/nas/share05/ops/mtnops/daily_reports.py -p HYNET_REVENUE_SHARE -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)

REV_PER_BTS_SUMD = BashOperator(
     task_id='REV_PER_BTS_SUMD' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p REV_PER_BTS_SUMD -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)

#NO_MGMT_SUMMARY = BashOperator(
#     task_id='No_Mgmt_Summary',
#     run_as_user = 'daasuser',
#     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p NOMGMT -l 3 ',
#     dag=dag,
#)

Daily_Number_Mgmt = BashOperator(
     task_id='Daily_Number_Mgmt' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p Daily_Number_Mgmt -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)

Daily_Device_Report = BashOperator(
     task_id='Daily_Device_Report' ,
     bash_command='perl /nas/share05/ops/daily/TPC_PACKAGE1_DeviceActivation.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser',
)


Daily_Device_Report >> MSC_CCN_RECON_ID >> GGSN_Traffic >> PARTNER_BALANCES >> Daily_Number_Mgmt >> RATED_VOICE >> RATED_SMS >> RATED_GPRS >> RATED_ADJ_MA >> RATED_REFILL_MA >> RATED_MSC >> RATED_GGSN >> RATED_VOICE_DA >> RATED_SMS_DA >> RATED_B4U_VOICE >> RATED_B4U_GPRS >> FILE_RECON_SUMMARY >>  HYNET_REVENUE_SHARE >> REV_PER_BTS_SUMD  >> CGI_Validation
