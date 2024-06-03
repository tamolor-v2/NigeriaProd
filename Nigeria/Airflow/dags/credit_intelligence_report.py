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
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
dag = DAG(
    dag_id='CREDIT_INTELLIGENCE_REPORT',
    default_args=args,
    schedule_interval='0 4 1,2 * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

RC_CREDIT_INT_STEP1 = BashOperator(
     task_id='RC_CREDIT_INT_STEP1',
     bash_command='/nas/share05/ops/mtnops/credit_intelligence_report.py -p RC_CREDIT_INT_STEP1',
     dag=dag,
     run_as_user = 'daasuser'
)

RC_CREDIT_INT_STEP2 = BashOperator(
     task_id='RC_CREDIT_INT_STEP2',
     bash_command='/nas/share05/ops/mtnops/credit_intelligence_report.py -p RC_CREDIT_INT_STEP2',
     dag=dag,
     run_as_user = 'daasuser'
)

RC_CREDIT_INT_STEP3 = BashOperator(
     task_id='RC_CREDIT_INT_STEP3', 
     bash_command='/nas/share05/ops/mtnops/credit_intelligence_report.py -p RC_CREDIT_INT_STEP3',
     dag=dag,
     run_as_user = 'daasuser'
)

RC_CREDIT_INT_STEP4 = BashOperator(
     task_id='RC_CREDIT_INT_STEP4',
     bash_command='/nas/share05/ops/mtnops/credit_intelligence_report.py -p RC_CREDIT_INT_STEP4',
     dag=dag,
     run_as_user = 'daasuser'
)

RC_CREDIT_INT_STEP5 = BashOperator(
     task_id='RC_CREDIT_INT_STEP5',
     bash_command='/nas/share05/ops/mtnops/credit_intelligence_report.py -p RC_CREDIT_INT_STEP5',
     dag=dag,
     run_as_user = 'daasuser'
)

CREDIT_INTELLIGENCE_REP = BashOperator(
     task_id='CREDIT_INTELLIGENCE_REP',
     bash_command='/nas/share05/ops/mtnops/credit_intelligence_report.py -p CREDIT_INTELLIGENCE_REP',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

DYA_POSTLINKED_AIRTIME = BashOperator(
     task_id='dya_postlinked_airtime',
     bash_command='perl /nas/share05/ops/monthly/dya_postlinked_airtime_steps.pl `date --date="-4 days" +%Y%m%d` `date --date="-4 days" +%Y%m%d` ',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


RC_CREDIT_INT_STEP1 >> RC_CREDIT_INT_STEP2 >> RC_CREDIT_INT_STEP3 >> RC_CREDIT_INT_STEP4 >> RC_CREDIT_INT_STEP5 >> CREDIT_INTELLIGENCE_REP >> DYA_POSTLINKED_AIRTIME
