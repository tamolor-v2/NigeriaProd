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
    'email': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


d_1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

dag = DAG(
    dag_id='ECW_RECONCILIATION',
    default_args=args,
    schedule_interval='0 23 * * * ',
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



financial_log_recon = BashOperator(
     task_id='financial_log_recon' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/mtnops/financial_log_recon.sh {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

mfs_audit_log = BashOperator(
     task_id='mfs_audit_log' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/mtnops/mfs_audit_log.sh {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

mfs_transacting_log_recon = BashOperator(
     task_id='mfs_transacting_log_recon' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/mtnops/mfs_transacting_log_recon.sh {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)


start >> [financial_log_recon, mfs_audit_log, mfs_transacting_log_recon]
