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
    'email': ['s.suliman@ligadata.com'],
    'email_on_failure': ['s.suliman@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


dag = DAG(
    dag_id='Terragon_Smart_Survey_Daily',
    default_args=args,
 schedule_interval='0 9 * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)
Terragon_Smart_Survey_Daily = BashOperator(
     task_id='Terragon_Smart_Survey_Daily' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Terragon_Smart_Survey/Terragon_Smart_Survey_Daily.sh	',
     dag=dag,
     run_as_user = 'daasuser'
)
Terragon_Smart_Survey_Daily
