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
    'owner': 'IMEI',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['a.jawawdeh@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['a.jawawdeh@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
IMEI_REPORT_C = DAG(
    dag_id='IMEI_REPORT_C',
    default_args=args,
    schedule_interval=' 30 6 * * * ',
    concurrency=1,
    max_active_runs=1
)

submission_commission = BashOperator(
     task_id='submission_commission',
     bash_command=' bash  /nas/share05/tools/IMEI_report/airflow_submission_commission.sh ',
     dag=IMEI_REPORT_C,
     run_as_user='daasuser'
)
#No_Mgmt_Summary = BashOperator(
#     task_id='No_Mgmt_Summary',
#     depends_on_past=True,
#     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p NOMGMT ',
#     dag=dag,
#)


submission_commission 
