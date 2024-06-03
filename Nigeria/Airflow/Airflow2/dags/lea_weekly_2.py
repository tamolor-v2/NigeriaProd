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
    dag_id='LEA_WEEKLY_2',
    default_args=args,
#    schedule_interval='0 18 * * *',
    schedule_interval=None,
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)

LEA_WEEKLY_2 = BashOperator(
     task_id='lea_weekly_2' ,
     bash_command='for DATE in 20181022 20181029 ;do ssh datanode01002 bash /mnt/beegfs_bsl/apidfs/apiscripts/LeaWklyFds_nas_hist.sh $DATE lea_all,imsi_lea,imei_lea true 2>&1 | tee /mnt/beegfs_bsl/apidfs/test/apilogs/LeaWklyBatchFds_Lea_Wkly_$(date +%Y%m%d_%s).txt; done  ',
     run_as_user = 'daasuser',
     dag=dag,
)

LEA_WEEKLY_2
