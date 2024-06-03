from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['n.najjar@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['n.najjar@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Extract_MADAPI_NPG_ORDER_FULFILLMENT',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

Extract_MADAPI_NPG_ORDER_FULFILLMENT = BashOperator(
     task_id='Extract_MADAPI_NPG_ORDER_FULFILLMENT' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/extract_MADAPI_NPG_ORDER_FULFILLMENT_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

Extract_MADAPI_NPG_ORDER_FULFILLMENT
