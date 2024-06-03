from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


dag = DAG(
    dag_id='SALES_HIERARCHY',
    default_args=args,
    schedule_interval='00 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)
Insert_SALES_HIERARCHY = BashOperator(
     task_id='Insert_SALES_HIERARCHY' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/ExtractTools/SALES_HIERARCHY.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

Insert_SALES_HIERARCHY
