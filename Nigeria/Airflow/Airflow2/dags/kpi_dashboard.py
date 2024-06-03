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
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='KPI_DASHBOARD',
    default_args=args,
    schedule_interval='5 5 * * *',
    catchup=False,  
    concurrency=2,
    max_active_runs=2

)

KPI_OLD = BashOperator(
     task_id='KPI_OLD' ,
     bash_command='bash /nas/share05/tools/kpi_reports/scripts/new/runallmanually.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)

KPI_NEW = BashOperator(
     task_id='KPI_NEW' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/kpi.py -l 8 -p ALL',
     run_as_user = 'daasuser',
     dag=dag,
)

KPI_MANUAL = BashOperator(
     task_id='KPI_MANUAL' ,
     bash_command='bash /nas/share05/tools/kpi_reports/scripts/new/manual_kpi_tosin.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)

KPI_OLD, [KPI_NEW >> KPI_MANUAL]
