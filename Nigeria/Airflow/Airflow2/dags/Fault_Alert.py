from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from pyhive import presto
import subprocess

args = {
	'owner': 'airflow',
    'depends_on_past':False,
	'start_date': datetime(2020,3,20),
	'email': ["y.bloukh@ligadata.com"],
	'email_on_failure': True,
	'email_on_retry': True,
	'email_on_success': True,
	'retries': 1,
	'retry_delay': timedelta(minutes=3),
}

dag = DAG('Fault_Alarm',
	default_args=args,
	schedule_interval= "55 */2 * * *",
	catchup=False,
	max_active_runs=1
)

Fault_alarm = BashOperator(
     task_id='Fault_alarm' ,
     bash_command='python3.6 /nas/share05/tools/Fault_Alarm/Fault_Alert.py ',
     dag=dag,
     run_as_user = 'daasuser'
)

