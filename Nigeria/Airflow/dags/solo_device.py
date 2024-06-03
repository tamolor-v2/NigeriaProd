from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators import BashOperator,PythonOperator
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
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')
dag = DAG(
    dag_id='SOLO_DEVICE',
    default_args=args,
    schedule_interval='0 10 * * *',
    description='SOLO_DEVICE',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)
RUN_SOLO_DEV = BashOperator(
     task_id='RUN_SOLO_DEVICE' ,
     bash_command='python3.6 /nas/share05/dataOps_dev/solo_device/solo_device_rpt.py {0} {0} 1 '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser')

