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
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')
dag = DAG(
    dag_id='DIGITAL_CHANNEL_RPT',
    default_args=args,
    schedule_interval='0 10 * * *',
    description='DIGITAL_CHANNEL_RPT',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)
RUN_SOLO_DEV = BashOperator(
     task_id='RUN_DIGITAL_CHANNEL_RPT' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/Digital_Channel.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)

