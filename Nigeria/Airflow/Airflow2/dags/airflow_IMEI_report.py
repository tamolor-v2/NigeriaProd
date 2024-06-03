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
 
IMEI_REPORT_DMC = DAG(
    dag_id='IMEI_REPORT_DMC',
    default_args=args,
    schedule_interval=' 0 2 * * * ',
    concurrency=1,
    max_active_runs=1
)

dedup_dmc = BashOperator(
     task_id='dedup_dmc',
     bash_command=' bash /nas/share05/tools/IMEI_report/airflow_dmc_dedup.sh ',
     dag=IMEI_REPORT_DMC,
     run_as_user='daasuser'
)

dim_handset = BashOperator(
     task_id='dim_handset',
     bash_command=' bash /nas/share05/tools/IMEI_report/airflow_dim_handset.sh ',
     dag=IMEI_REPORT_DMC,
     run_as_user='daasuser'
)
imei_tracker = BashOperator(
     task_id='imei_tracker',
     bash_command=' bash /nas/share05/tools/IMEI_report/airflow_imei_tracker.sh ',
     dag=IMEI_REPORT_DMC,
     run_as_user='daasuser'
)
#No_Mgmt_Summary = BashOperator(
#     task_id='No_Mgmt_Summary',
#     depends_on_past=True,
#     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p NOMGMT ',
#     dag=dag,
#)


dedup_dmc >> [dim_handset , imei_tracker]


