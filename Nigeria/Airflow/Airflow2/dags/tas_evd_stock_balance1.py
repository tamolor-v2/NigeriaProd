from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
dag = DAG(
    dag_id='tas_evd_stock_balance1',
    default_args=args,
    schedule_interval='10 0-23 * * *',
    description='tas_evd_stock_balance',
    catchup=False,
    concurrency=3,
    max_active_runs=3
)
Run_tas_evd_stock_bal = BashOperator(
     task_id='tas_evd_stock_balance' ,
     bash_command='ssh datanode01038;bash /nas/share05/tools/DBExtrct/automated_scripts/Tas_Evd_Stock_Balance.sh ' ,
     dag=dag, run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

