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
    'email': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='GAGC', 
    default_args=args,
    schedule_interval='0 5 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

GAGC= BashOperator(
     task_id='gagc' ,
     bash_command='perl /nas/share05/ops/daily/TPC_PACKAGE1_DeviceActivation.pl `date --date="-1 days" +%Y%m%d` ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

USSD_SUMM= BashOperator(
     task_id='ussd_summ' ,
     bash_command='bash /nas/share05/dataOps_prod/ussd_logs_generator/summary_oneoff.sh `date --date="-1 days" +%Y%m%d` ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SPONS_DATA= BashOperator(
     task_id='spons_data' ,
     bash_command='python3.6 /nas/share05/dataOps_prod/platform_revenue/usage_summary.py -p SPONS_DATA -l 2 -s `date --date="-1 days" +%Y-%m-%d`',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

#MSC_RECON= BashOperator(
#     task_id='msc_recon' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/daily_reports.py -p MSC_CCN_RECON -l 3 ',
  #depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#CS18_MSC_RECON= BashOperator(
#     task_id='cs18_msc_recon' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p cs18_msc_ccn_recon -l 3 ',
  #depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser'
#)
#CS18_MSC_TREND= BashOperator(
#     task_id='cs18_msc_trend' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/cs18_ops_reports.py -p MSC_CCN_TREND -l 3 ',
  #depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#MSC_TREND= BashOperator(
#     task_id='msc_trend' ,
#     bash_command='perl /nas/share05/ops/daily/msc_ccn_trend.pl ',
  #depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser'
#)

GAGC >> USSD_SUMM >> SPONS_DATA #>> MSC_RECON >> CS18_MSC_RECON >> CS18_MSC_TREND
 
