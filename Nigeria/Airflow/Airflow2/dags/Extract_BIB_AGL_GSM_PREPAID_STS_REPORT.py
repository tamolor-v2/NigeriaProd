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
    dag_id='Extract_BIB_AGL_GSM_PREPAID_STS_REPORT',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

Extract_BIB_AGL_GSM_PREPAID_STS_REPORT = BashOperator(
     task_id='Extract_BIB_AGL_GSM_PREPAID_STS_REPORT' ,
     bash_command='bash /nas/share05/tools/ExtractTools/BIB_AGL_GSM_PREPAID_STS_REPORT/extract_BIB_AGL_GSM_PREPAID_STS_REPORT.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

Extract_BIB_AGL_GSM_PREPAID_STS_REPORT
