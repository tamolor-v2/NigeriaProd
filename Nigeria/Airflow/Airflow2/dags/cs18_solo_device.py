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
    'retries': 8,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='CS18_Daily_Solo_Device',
    default_args=args,
    schedule_interval='* 5-7 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)
SOLO_DEVICE_TAB = BashOperator(
     task_id='Financial_Log' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p SOLO_RECORDS -l 5',
     dag=dag,
     run_as_user = 'daasuser',
)

CS5_SOLO_DEVICE_TAB = BashOperator(
     task_id='cs5_solo' ,
     depends_on_past=False,
     bash_command='python3.6 /nas/share05/dataOps_dev/solo_device/solo_device_rpt_prod.py `date --date="-5 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` 1',
     dag=dag,
     run_as_user = 'daasuser',
)

SOLO_DEVICE_TAB 

