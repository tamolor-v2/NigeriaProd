from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
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
    dag_id='digital_payment_api_d_0',
    default_args=args,
    schedule_interval='0 3,11,15,19,23 * * *',
    description='digital_payment_d-0',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)
RUN_digital_payment_d0 = BashOperator(
     task_id='RUN_digital_payment_d-0' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/Digital_Payment.sh `date --date="-0 days" +%Y%m%d` ' ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=3)

