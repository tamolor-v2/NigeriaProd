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
    'email': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_failure': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': timedelta(minutes=60),
    'catchup':False,
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
}



#date_param = (monthdelta(date.today(), -6)).strftime('%Y%m%d')


dag = DAG(
    dag_id='promo_code_customer_profiling',
    default_args=args,
    schedule_interval='30 12 10 1,4,7,10 *',
    description='segmenting the customers into four different bands.  based on the average number of Data bundles they purchased in the last 90 days',    
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


input_validation = BashOperator(
    task_id='input_validation',
    bash_command= 'bash /nas/share05/scripts/promo_code/customer_profiling_table/customer_profiling.sh `date --date="-1 days" +%Y%m%d` input_validation',
    dag=dag,
    run_as_user='daasuser')
report_script = BashOperator(
    task_id='report_script',
    bash_command= 'bash /nas/share05/scripts/promo_code/customer_profiling_table/customer_profiling.sh `date --date="-1 days" +%Y%m%d` report_script',
    dag=dag,
    run_as_user='daasuser')
input_validation >> report_script 

