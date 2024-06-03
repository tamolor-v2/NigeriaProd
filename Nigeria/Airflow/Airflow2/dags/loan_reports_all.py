from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y%m')
#date_param1 = (datetime.now() - timedelta(days=31)).strftime('%Y%m')
dag = DAG(
    dag_id='loan_reports_all',
    default_args=args,
    schedule_interval='0 6 4 * *',
    description='loan_reports_all',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)
RUN_loan_reorts = BashOperator(
     task_id='loan_report_1' ,
     bash_command='python3.6 /nas/share05/dataOps_dev/loan_recovery/loan_reports_all.py {0} {0} 0 '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=1),retries=2)

