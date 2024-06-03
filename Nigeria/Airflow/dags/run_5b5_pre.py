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
    'email': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')

 
 
dag = DAG(
    dag_id='RUN_5B5_PRE',
    default_args=args,
    schedule_interval='0 18 * * *',
    description='D-0 PRE STEPS FOR NEXT DAY 5B5 RUN',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)


RUN_5B5_DEV = BashOperator(
     task_id='RUN_5B5_DEV' ,
     bash_command='/nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {0} segment5b5_dev_main daily 8668'.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser'

)

RUN_SUBPRED_1 = BashOperator(
     task_id='RUN_SUBPRED_1' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subpred1 daily 8668'.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser'


)

RUN_SUBPRED_2 = BashOperator(
     task_id='RUN_SUBPRED_2' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subpred2 daily 8668'.format(date_param) ,
     run_as_user='daasuser',
     dag=dag,
)


RUN_5B5_DEV >> RUN_SUBPRED_1 >> RUN_SUBPRED_2


