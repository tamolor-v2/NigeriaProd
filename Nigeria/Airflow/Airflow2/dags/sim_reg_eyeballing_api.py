from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
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
    dag_id='sim_reg_eyeballing_api',
    default_args=args,
    schedule_interval='00 07 * * *',
    description='Sim_Reg_Eyeballing_api',
    catchup=False,
    concurrency=3,
    max_active_runs=3
)
start_dag = BashOperator(
    task_id='start_dag',
    bash_command='echo start_dag ',
    dag=dag,
    run_as_user = 'daasuser')

USSD_REPORT = BashOperator(
     task_id='Sim_Reg_Eyeballing_api' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/Sim_Reg_Eyeballing.sh ',
     dag=dag,
     run_as_user = 'daasuser',
)

USSD_REPORT_INSERT = BashOperator(
     task_id='USSD_REPORT_INSERT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p USSD_REPORT -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)
start_dag >>USSD_REPORT >> USSD_REPORT_INSERT
