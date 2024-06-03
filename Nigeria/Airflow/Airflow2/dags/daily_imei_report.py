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
    dag_id='daily_imei_report',
    default_args=args,
    schedule_interval='0 07 * * *',
    description='daily_imei_report',
    catchup=False,
    concurrency=3,
    max_active_runs=3
)
start_dag = BashOperator(
    task_id='start_dag',
    bash_command='echo start_dag ',
    dag=dag,
    run_as_user = 'daasuser')

daily_imei_report = BashOperator(
     task_id='daily_imei_report' ,
     bash_command='python3.6 /nas/share05/dataOps_dev/daily_imei_spool/daily_imei_spool.py {0} {0} 0 '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser',
)
start_dag >>daily_imei_report
