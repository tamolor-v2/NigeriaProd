from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import calendar
from datetime import date


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}


date_param = (datetime.now() - timedelta(days=4)).strftime('%Y%m%d')
date_param_1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
#


dag = DAG(
    dag_id='SERVICE_CLASS_49_MIGRATION',
    default_args=args,
    schedule_interval='30 7 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)



check_sub = BashOperator(
     task_id='check_sub' ,
     bash_command='bash /nas/share05/ops/mtnops/clm_cvm_sub_check.sh {0}'.format(date_param_1),
     dag=dag,
     run_as_user = 'daasuser'
)

SERVICE_CLASS_49_MIGRATION = BashOperator(
     task_id='SERVICE_CLASS_49_MIGRATION' ,
     bash_command='bash /nas/share05/ops/daily/service_class_49_migration.sh {0} {1}'.format(date_param_1,date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

SERVICE_CLASS_MIGRATION = BashOperator(
     task_id='SERVICE_CLASS_MIGRATION' ,
     bash_command='bash /nas/share05/ops/daily/service_class_migration.sh {0} {1}'.format(date_param_1,date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


check_sub >> SERVICE_CLASS_49_MIGRATION >> SERVICE_CLASS_MIGRATION
