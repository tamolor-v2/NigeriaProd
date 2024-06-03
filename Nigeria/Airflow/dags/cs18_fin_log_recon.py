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
    dag_id='CS18_Financial_Log',
    default_args=args,
    schedule_interval='* 6 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

ncc_rgs = BashOperator(
     task_id='fin_log' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p FIN_LOG ',
     dag=dag,
     run_as_user = 'daasuser',
)
ncc_rgs


ncc_rgs
