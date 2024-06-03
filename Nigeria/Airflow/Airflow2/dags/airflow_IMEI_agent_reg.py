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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['hendre@ligadata.com','a.jawawdeh@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['hendre@ligadata.com','a.jawawdeh@ligadata.com','support@ligadata.com'],
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

IMEI_REPORT_AGENT_REGISTRATION = DAG(
    dag_id='IMEI_REPORT_AGENT_REGISTRATION',
    default_args=args,
    schedule_interval=' 50 4 * * * ',
    concurrency=1,
    max_active_runs=1
)

extract_registration = BashOperator(
     task_id='extract_registration',
     bash_command=' bash  /nas/share05/tools/ExtractTools/tbl_data_agent_registration_vw_LIVE_N/tbl_data_agent_registration_vw_LIVE_N.sh ',
     dag=IMEI_REPORT_AGENT_REGISTRATION,
     run_as_user='daasuser'
)

delete_d_2_registration = BashOperator(
     task_id='delete_d_2_registration',
     bash_command=' bash  /nas/share05/tools/ExtractTools/tbl_data_agent_registration_vw_LIVE/check_delete_partion.sh ',
     dag=IMEI_REPORT_AGENT_REGISTRATION,
     run_as_user='daasuser'
)

msck_d_1_registration = BashOperator(
     task_id='msck_d_1_registration',
     bash_command=' bash  /nas/share05/tools/msckrepair/msck_N_IMEI.sh ',
     dag=IMEI_REPORT_AGENT_REGISTRATION,
     run_as_user='daasuser'
)

extract_registration >> delete_d_2_registration >> msck_d_1_registration

