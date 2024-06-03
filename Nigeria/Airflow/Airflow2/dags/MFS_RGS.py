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
    'email': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='MFS_RGS',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    max_active_runs=1
)
Insert_MFS_RGS = BashOperator(
     task_id='Insert_MFS_RGS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/ExtractTools/MFS_RGS.sh `date --date="-1 days" +%Y%m%d`	'  ,
     dag=dag,
     run_as_user = 'daasuser'
)
