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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='DAAS7_API_OneFeed',
    default_args=args,
    schedule_interval='30 8,12 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

da_history = BashOperator(
     task_id='da_history' ,
     #bash_command='bash /mnt/beegfs_bsl/apidfs/apiscripts/DAAS7_API_OneFeed.sh $(date -d '-1 day' +%Y%m%d) da_history',
     #run_as_user = 'daasuser',
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/DAAS7_API_OneFeed.sh `date --date="-1 days" +%Y%m%d` da_history  ',
     run_as_user = 'daasuser',     
     dag=dag,
)

last_recharge = BashOperator(
     task_id='last_recharge' ,
     #bash_command='bash /mnt/beegfs_bsl/apidfs/apiscripts/DAAS7_API_OneFeed.sh $(date -d '-1 day' +%Y%m%d) last_recharge',
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/DAAS7_API_OneFeed.sh `date --date="-1 days" +%Y%m%d` last_recharge  ',     
     run_as_user = 'daasuser',
     dag=dag,
)

da_history >> last_recharge
