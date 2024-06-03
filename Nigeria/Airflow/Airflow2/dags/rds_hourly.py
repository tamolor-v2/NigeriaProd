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
    dag_id='rds_feeds',
    default_args=args,
    schedule_interval='0 * * * *',
    description='rds_feeds',
    catchup=False,
    concurrency=10,
    max_active_runs=10
)
start_dag = BashOperator(
    task_id='start_dag',
    bash_command='echo start_dag ',
    dag=dag,
    run_as_user = 'daasuser')

ACCOUNT_HOLDER = BashOperator(
     task_id='ACCOUNT_HOLDER' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/ACCOUNT_HOLDER/extract_ACCOUNT_HOLDER.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)
ACCOUNT_REFERENCE = BashOperator(
     task_id='ACCOUNT_REFERENCE' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/ACCOUNT_REFERENCE/extract_ACCOUNT_REFERENCE.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

AHIDENTITIES = BashOperator(
     task_id='AHIDENTITIES' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/AHIDENTITIES/extract_AHIDENTITIES.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

AUDITTRAIL= BashOperator(
     task_id='AUDITTRAIL' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/AUDITTRAIL/extract_AUDITTRAIL.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

FINANCIALRECEIPT = BashOperator(
     task_id='FINANCIALRECEIPT' ,
     bash_command='bash /nas/share05/tools/ExtractTools/RDS_FEEDS/FINANCIALRECEIPT/extract_FINANCIALRECEIPT.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

FINANCIALRECEIPT_DETAILS = BashOperator(
     task_id='FINANCIALRECEIPT_DETAILS' ,
     bash_command='/nas/share05/tools/ExtractTools/RDS_FEEDS/FINANCIALRECEIPT_DETAILS/extract_FINANCIALRECEIPT_DETAILS.sh ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)
SESSIONLOGEVENT= BashOperator(
     task_id='SESSIONLOGEVENT' ,
     bash_command='/nas/share05/tools/ExtractTools/RDS_FEEDS/SESSIONLOGEVENT/extract_SESSIONLOGEVENT.sh  ',
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)
start_dag>>ACCOUNT_HOLDER
start_dag>>ACCOUNT_REFERENCE
start_dag>>AHIDENTITIES
start_dag>>AUDITTRAIL
start_dag>>FINANCIALRECEIPT
start_dag>>FINANCIALRECEIPT_DETAILS
start_dag>>SESSIONLOGEVENT
