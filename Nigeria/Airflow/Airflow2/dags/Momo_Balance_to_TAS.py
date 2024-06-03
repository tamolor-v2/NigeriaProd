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
    'depends_on_past':False,
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
    dag_id='Momo_Balance_to_TAS',
    default_args=args,
    schedule_interval='5 0-23 * * *',
    description='Momo_Balance_to_TAS',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)
RUN_SOLO_DEV = BashOperator(
     task_id='RUN_Momo_Balance_to_TAS' ,
     bash_command='bash /nas/share05/tools/DBExtrct/automated_scripts/Momo_Balance_to_TAS.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

