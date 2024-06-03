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
    'email': ['s.adamson@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['s.adamson@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_param1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

today = datetime.today()
dateRun = datetime.today() + timedelta(weeks=int(-1))
dateRunStr = dateRun.strftime('%Y%m01')

dag = DAG(
    dag_id='DATAOPS_MONTHLY_RPT',
    default_args=args,
    schedule_interval='0 0 1 * *',
    description='DATAOPS_MONTHLY_REPROT',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)

POSTPAID_CREDIT_SCORE = BashOperator(
     task_id='POSTPAID_CREDIT_SCORE' ,
     bash_command='python3.9 /nas/share05/dataOps_prod/ops/dataops_jobs.py -p CONSUMER_POSTPAID_CS -l 1 -s `date --date="-1 days" +%Y-%m-%d` '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)


POST_PAID_CREDIT_SCORE_API = BashOperator(
    task_id='POST_PAID_CREDIT_SCORE_API' ,
    bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/Credit_Score.sh {0} '.format(dateRunStr),
    run_as_user='daasuser',
    dag=dag,
)

CONNECT_FLEX = BashOperator(
    task_id='CONNECT_FLEX_CUG' ,
    bash_command='python3.9 /nas/share05/dataOps_prod/ops/dataops_jobs.py -p CONNECT_FLEX -l 1 -s `date --date="-0 days" +%Y-%m-%d` '.format(dateRunStr),
    run_as_user='daasuser',
    dag=dag,
)

FLYTXT_CAMPAIGN_POSTPAID = BashOperator(
    task_id='FLYTXT_CAMPAIGN_POSTPAID' ,
    bash_command='bash /nas/share05/ops/scripts/FLYTXT_CAMPAIGN_CONSUMER_POSTPAID/FLYTXT_CAMPAIGN_CONSUMER_POSTPAID.sh {0} '.format(dateRunStr),
    run_as_user='daasuser',
    dag=dag,
)

POSTPAID_CREDIT_SCORE >> [POST_PAID_CREDIT_SCORE_API,FLYTXT_CAMPAIGN_POSTPAID]
CONNECT_FLEX