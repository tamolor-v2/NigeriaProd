from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
#from airflow.operators import BashOperator,PythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
dag = DAG(
    dag_id='rds_feeds_rerun_yest',
    default_args=args,
    schedule_interval='30 6 * * *',
    description='rds_feeds',
    catchup=False,
    concurrency=5,
    max_active_runs=5
)
start_dag = BashOperator(
    task_id='start_dag',
    bash_command='echo start_dag ',
    dag=dag,
    run_as_user = 'daasuser')

ACCOUNT_HOLDER_rerun_yest = BashOperator(
     task_id='ACCOUNT_HOLDER_rerun_yest' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/ACCOUNT_HOLDER/extract_ACCOUNT_HOLDER_rerun_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)
ACCOUNT_REFERENCE_rerun_yest = BashOperator(
     task_id='ACCOUNT_REFERENCE_rerun_yest' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/ACCOUNT_REFERENCE/extract_ACCOUNT_REFERENCE_rerun_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

AHIDENTITIES_rerun_yest = BashOperator(
     task_id='AHIDENTITIES_rerun_yest' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/AHIDENTITIES/extract_AHIDENTITIES_rerun_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

AUDITTRAIL_rerun_yest= BashOperator(
     task_id='AUDITTRAIL_rerun_yest' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/AUDITTRAIL/extract_AUDITTRAIL_rerun_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

FINANCIALRECEIPT_rerun_yest = BashOperator(
     task_id='FINANCIALRECEIPT_rerun_yest' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/FINANCIALRECEIPT/extract_FINANCIALRECEIPT_rerun_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

FINANCIALRECEIPT_DETAILS_rerun_yest = BashOperator(
     task_id='FINANCIALRECEIPT_DETAILS_rerun_yest' ,
     bash_command='/nas/share05/tools/ExtractTools/RDS_FEEDS/FINANCIALRECEIPT_DETAILS/extract_FINANCIALRECEIPT_DETAILS_rerun_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)
SESSIONLOGEVENT_rerun_yest= BashOperator(
     task_id='SESSIONLOGEVENT_rerun_yest' ,
     bash_command='/nas/share05/tools/ExtractTools/RDS_FEEDS/SESSIONLOGEVENT/extract_SESSIONLOGEVENT_rerun_yest.sh  ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)
start_dag>>ACCOUNT_HOLDER_rerun_yest
start_dag>>ACCOUNT_REFERENCE_rerun_yest
start_dag>>AHIDENTITIES_rerun_yest
start_dag>>AUDITTRAIL_rerun_yest
start_dag>>FINANCIALRECEIPT_rerun_yest
start_dag>>FINANCIALRECEIPT_DETAILS_rerun_yest
start_dag>>SESSIONLOGEVENT_rerun_yest
