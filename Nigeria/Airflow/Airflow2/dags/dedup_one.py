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
    dag_id='CS18_Cluster_Remove_Duplicates_Events',
    default_args=args,
    schedule_interval='* 20,6 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

BEEP_CALL = BashOperator(
     task_id='CS18_Remov_Duplicates_Events' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/pr/cs18_reports.py -p dedup -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
BEEP_CALL


BEEP_CALL
