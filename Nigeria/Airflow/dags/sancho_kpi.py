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
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','ayodeji.shadare@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


d_1 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

dag = DAG(
    dag_id='SANCHO_KPI',
    default_args=args,
    schedule_interval='0 8 * * * ',
    catchup=False,
    concurrency=5,
    max_active_runs=5
)

start = BashOperator(
     task_id='start' ,
     bash_command='echo start',
     dag=dag,
     run_as_user = 'daasuser'
)

128

ACTIVE_DATA_KPI = BashOperator(
     task_id='ACTIVE_DATA_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p ACTIVE_DATA_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

MFS_REVENUE_KPI = BashOperator(
     task_id='MFS_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p MFS_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

DATA_REVENUE_KPI = BashOperator(
     task_id='DATA_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p DATA_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

MKT_SHARE_KPI = BashOperator(
     task_id='MKT_SHARE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p MKT_SHARE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

OTHER_KPI = BashOperator(
     task_id='OTHER_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p OTHER_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

ROAM_REVENUE_KPI = BashOperator(
     task_id='ROAM_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p ROAM_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

SMS_REVENUE_KPI = BashOperator(
     task_id='SMS_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p SMS_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

VAS_REVENUE_KPI = BashOperator(
     task_id='VAS_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p VAS_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

VOICE_REVENUE_KPI = BashOperator(
     task_id='VOICE_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p VOICE_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

XTRATIME_REVENUE_KPI = BashOperator(
     task_id='XTRATIME_REVENUE_KPI' ,
     bash_command='/nas/share05/ops/mtnops/sancho_kpi.py -p XTRATIME_REVENUE_KPI -l 1 -s {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

start >> [ACTIVE_DATA_KPI, MFS_REVENUE_KPI, DATA_REVENUE_KPI, MKT_SHARE_KPI, OTHER_KPI, ROAM_REVENUE_KPI, SMS_REVENUE_KPI, VAS_REVENUE_KPI, VOICE_REVENUE_KPI, XTRATIME_REVENUE_KPI ]
