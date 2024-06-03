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
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='SRTT_2',
    default_args=args,
    schedule_interval='0 8 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

start_sms1 = BashOperator(
     task_id='start_sms' ,
     bash_command='perl /nas/share05/ops/mtnops/sms_alert/srtt1.pl',
     dag=dag,
     run_as_user = 'daasuser'
)

SRTT_CLOSED = BashOperator(
     task_id='LOAD_DSSA_SRTT_CLOSED' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SRTT_CLOSED -l 1 -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

SRTT_CREATED = BashOperator(
     task_id='LOAD_DSSA_SRTT_CREATED' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SRTT_CREATED -l 1 -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

END_SMS2 = BashOperator(
     task_id='END_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/sms_alert/report_sms2.sh  ',
     dag=dag,
     run_as_user = 'daasuser',
)


start_sms1 >> SRTT_CLOSED >> SRTT_CREATED >> END_SMS2
