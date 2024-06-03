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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com','t.adigun@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','t.adigun@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='FIN_AUD_LOG',
    default_args=args,
    schedule_interval='0 8 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


FINANCIAL_LOG = BashOperator(
     task_id='financial_log' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/daily/financial_log.sh `date --date="-1 days" +%Y%m%d`    ',
     dag=dag,
     run_as_user = 'daasuser'
)

AUDIT_LOG = BashOperator(
     task_id='audit_log' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/daily/audit_log.sh `date --date="-1 days" +%Y%m%d`     ',
     dag=dag,
     run_as_user = 'daasuser'
)

MFS_LOG = BashOperator(
     task_id='mfs_log' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/daily/mfs_log.sh `date --date="-1 days" +%Y%m%d`     ',
     dag=dag,
     run_as_user = 'daasuser'
)


FAILED_ACT = BashOperator(
     task_id='failed_act' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/daily/failed_data_bundle_activation.sh `date --date="-1 days" +%Y%m%d`     ',
     dag=dag,
     run_as_user = 'daasuser'
)

NIN_HVC = BashOperator(
     task_id='nin_hvc' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/daily/hvc_nin_submission.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


FINANCIAL_LOG >> AUDIT_LOG >> MFS_LOG >> FAILED_ACT

