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
    dag_id='CS18_Daily_Audit_Reports',
    default_args=args,
    schedule_interval='* 5 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)
FIN_LOG = BashOperator(
     task_id='Financial_Log' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p FIN_LOG -l 2',
     dag=dag,
     run_as_user = 'daasuser',
)
FIN_LOG

AUDIT_LOG_RECON = BashOperator(
     task_id='Audit_Log' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p AUDIT_LOG_RECON -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
AUDIT_LOG_RECON

ECW_LOG_RECON = BashOperator(
     task_id='ECW_Log' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p ECW_LOG_RECON -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)
ECW_LOG_RECON

FIN_LOG >> ECW_LOG_RECON >> AUDIT_LOG_RECON 
