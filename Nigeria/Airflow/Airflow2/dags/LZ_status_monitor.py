from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date': datetime(2020,10,31),
    'email': 'm.abdin@ligadata.com',
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('LZ_status_monitor',
    default_args=args,
#    schedule_interval= " */30 * * * * ",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
          )

checkStatus = BashOperator(
    task_id = 'checkStatus',
    bash_command = 'bash /nas/share05/tools/Zabbix_Integration/bin/SyncLZ.sh ',
    dag=dag,
    
    queue='edge01002',
    run_as_user = 'daasuser'
) 

checkStatus
