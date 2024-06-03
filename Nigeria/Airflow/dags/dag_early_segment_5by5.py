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
    'start_date': datetime(2019, 11, 13),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='SEGMENT_5BY5_EARLY_RUN',
    default_args=args,
#    schedule_interval='0 3 * * *',
    schedule_interval=None,
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

SEGMENT_5BY5_EARLY_R = BashOperator(
     task_id='5by5_early_run' ,
     bash_command='nohup bash /mnt/beegfs_bsl/scripts/segment5b5/run_segment5b5.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` segment5b5_main2 daily 8099 > /dev/null &',
     dag=dag,
     run_as_user = 'daasuser'
)

SEGMENT_5BY5_EARLY_R
