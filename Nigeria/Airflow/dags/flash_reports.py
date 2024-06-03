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
    dag_id='Daily_Flash_Reports',
    default_args=args,
    schedule_interval='0 3 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

is_flash_prepaid_bal_per_sc = BashOperator(
     task_id='is_flash_prepaid_bal_per_sc' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_prepaid_bal_per_sc.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag,
     run_as_user = 'daasuser'
)

is_flash_refill_value = BashOperator(
     task_id='is_flash_refill_value' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_refill_value.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag,
     run_as_user = 'daasuser'
)

is_flash_sms_summary = BashOperator(
     task_id='is_flash_sms_summary' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_sms_summary.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag,
     run_as_user = 'daasuser'
)

is_flash_voice_summary = BashOperator(
     task_id='is_flash_voice_summary' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_voice_summary.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag,
     run_as_user = 'daasuser'
)

is_flash_refill_ma = BashOperator(
     task_id='is_flash_refill_ma' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_refill_ma.pl  `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag,
     run_as_user = 'daasuser'
)
    
is_flash_daily_usage = BashOperator(
     task_id='is_flash_daily_usage' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_daily_usage.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
     dag=dag
)

is_flash_deleted_total = BashOperator(
     task_id='is_flash_deleted_total' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_deleted_total.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`       ',
     dag=dag
)

is_flash_subscriber_status_main = BashOperator(
     task_id='is_flash_subscriber_status_main' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_subscriber_status_main.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`       ',
     dag=dag
)

is_flash_freebiz_analysis = BashOperator(
     task_id='is_flash_freebiz_analysis' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_freebiz_analysis.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`       ',
     dag=dag
)

IS_FLASH_GC_vs_RGS = BashOperator(
     task_id='IS_FLASH_GC_vs_RGS' ,
     bash_command='perl /nas/share05/ops/daily/Daas_IS_FLASH_Daily_GC_vs_RGS_v4.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

MONTHLY_SMS_TRAFFIC = BashOperator(
     task_id='monthly_sms_traffic' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_voice_and_sms_traffic.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

XTRATIME_AND_BYTES = BashOperator(
     task_id='xtratime_and_bytes' ,
     bash_command='perl /nas/share05/ops/daily/is_flash_xtratime_xtratime.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

is_flash_prepaid_bal_per_sc >> is_flash_refill_value >> is_flash_sms_summary >> is_flash_voice_summary >> is_flash_refill_ma >> is_flash_daily_usage >> is_flash_deleted_total >> is_flash_subscriber_status_main >> is_flash_freebiz_analysis >> IS_FLASH_GC_vs_RGS >> MONTHLY_SMS_TRAFFIC >> XTRATIME_AND_BYTES
