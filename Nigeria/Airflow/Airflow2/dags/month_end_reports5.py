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
    'email': ['t.adigun@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Month-End-REPORTS_5',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

SOCIAL_BUNDLE = BashOperator(
     task_id='social_bundle' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SOCIAL_BUNDLE',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


SNAP = BashOperator(
     task_id='snap' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  SUBS_SNAPSHOT -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SMS_REVENUE= BashOperator(
     task_id='sms_revenue' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  SMS_REVENUE -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


ROAM_REVE= BashOperator(
     task_id='roam_reve' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/roaming_rev.py `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


USSD_RATING= BashOperator(
     task_id='ussd_rating' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  USSD_RATING -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

POSTPAID_USAGE_SUMMARY= BashOperator(
     task_id='postpaid_usage_summary' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  POSTPAID_USAGE_SUMMARY -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

VAS_SHORTCODE= BashOperator(
     task_id='vas_shortcode' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  VAS_SHORTCODE -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SME_DATA= BashOperator(
     task_id='sme_data' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  SME_DATA -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SUPERVALUE= BashOperator(
     task_id='supervalue' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  SUPERVALUE -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

ONEBOX= BashOperator(
     task_id='onebox' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  ONEBOX -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)
SHIKENA= BashOperator(
     task_id='shikena' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p  SHIKENA -l 2 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)
#IDR= BashOperator(
#     task_id='idr' ,
#     bash_command='perl /nas/share05/ops/monthly/international_dialing_revenue.pl  `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` ',
  #depends_on_past=True,
#     dag=dag,
#     run_as_user = 'daasuser'
#)




SOCIAL_BUNDLE >> SNAP >> SMS_REVENUE >> ROAM_REVE >> USSD_RATING >> POSTPAID_USAGE_SUMMARY >> VAS_SHORTCODE >> SME_DATA >> SUPERVALUE >> ONEBOX >> SHIKENA
