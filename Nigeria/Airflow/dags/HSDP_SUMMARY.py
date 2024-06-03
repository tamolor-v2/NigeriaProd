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
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'concurrency' : 1,
    'catchup':False,
}

d_1 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
d_2 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
d_3 = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

dag = DAG(
    dag_id='HSDP_SUMMARY',
    default_args=args,
    schedule_interval='* 4,12,18 * * *',
    catchup=False, 
    concurrency=2,
    max_active_runs=1

)
t1 = BashOperator(
     task_id='hsdp_sumd1' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HSDP_SUMD -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

t4 = BashOperator(
     task_id='hsdp_sumd2' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HSDP_SUMD -l 1 -s {0} '.format(d_2),
     dag=dag,
     run_as_user = 'daasuser'
)

t5 = BashOperator(
     task_id='hsdp_sumd3' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HSDP_SUMD -l 1 -s {0} '.format(d_3),
     dag=dag,
     run_as_user = 'daasuser'
)


t2 = BashOperator(
     task_id='HYNET_OFFERS' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HYNET_OFFERS -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)

t3 = BashOperator(
     task_id='vas_revenue' ,
     bash_command='perl /nas/share05/ops/daily/mkt_ent_vas_revenue_metrics_v8.pl',
     dag=dag,
     run_as_user = 'daasuser'
)


#t6 = BashOperator(
#     task_id='vas_revenue_v10' ,
#     bash_command='perl /nas/share05/ops/daily/mkt_ent_vas_revenue_metrics_v10.pl',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

t1 >> t4 >> t5 >> [t2,t3]
#t3 >> t6

