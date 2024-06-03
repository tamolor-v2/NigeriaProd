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
    dag_id='Daily_Solo_Script',
    default_args=args,
    schedule_interval='0 7 * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)
Daily_Solo_Script = BashOperator(
     task_id='Daily_Solo_Script' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/SOLO_SCRIPTS/Daily_Solo_Script_Driver.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

Daily_Solo_Script
