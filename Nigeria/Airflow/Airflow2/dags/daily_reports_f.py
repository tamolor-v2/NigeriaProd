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
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'concurrency' : 1,
}

dag = DAG(
    dag_id='Daily_Reports_f',
    default_args=args,
    schedule_interval='55 4 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1

)
start_sms = BashOperator(
     task_id='start_sms' ,
     bash_command='bash /mnt/beegfs_bsl/ops/sms_alert/start_rpt_sms.sh',
     dag=dag,
     run_as_user = 'daasuser'
)


GDS_USAGE_REPORT = BashOperator(
     task_id='gds_usage_report' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/gds_usage_report.py `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

Sponsored_Data = BashOperator(
     task_id='Sponsored_Data' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p SPONSORED ',
     dag=dag,
     run_as_user = 'daasuser'
)

USSD_Summary = BashOperator(
     task_id='USSD_Summary' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p ussd ',
     dag=dag,
     run_as_user = 'daasuser'
)

MSISDN_SC_MA_DA_NEW = BashOperator(
     task_id='msisdn_sc_ma_da_new' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p MSISDN_SC_MA_DA_NEW ',
     dag=dag,
     run_as_user = 'daasuser',
)


END_SMS = BashOperator(
     task_id='END_SMS' ,
     bash_command='bash /mnt/beegfs_bsl/ops/sms_alert/report_sms1.sh',
     dag=dag,
     run_as_user = 'daasuser',
)
start_sms >> GDS_USAGE_REPORT >> Sponsored_Data >> USSD_Summary >> MSISDN_SC_MA_DA_NEW >> END_SMS
