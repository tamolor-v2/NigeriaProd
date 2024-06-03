import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils import timezone
from os import popen

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = yesterday.strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=2)
hour_run = hour_run_date.strftime('%H')
sleeptime= 2*1000*60
##sleeptime= 1000*10
currentdate = d_1
DATE = "${DATE}"
Delemeter = "'|'"

args = {
    'owner': 'daasuser',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['r.flefil@ligadata.com,support@ligadata.com'],
    'email_on_failure': ['r.flefil@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0),
    'catchup':False,
}


dag = DAG(
    dag_id='Crontab_Backup_Job',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=5,
    max_active_runs=1
)

backup_job_airflow="bash /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/backup_job_airflow.sh "
logging.info(backup_job_airflow)

remove_cron_backup_D_3 = BashOperator(
     task_id='remove_cron_backup_D_3' ,
     depends_on_past=False,
     bash_command='rm /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup_D-3.txt',
     dag=dag,
     run_as_user = 'daasuser'
)

copy_cron_backup_D_2_to_D_3 = BashOperator(
     task_id='copy_cron_backup_D_2_to_D_3',
     depends_on_past=False,
     bash_command='cp /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup_D-2.txt /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup_D-3.txt',
     dag=dag,
     run_as_user = 'daasuser'
)

copy_cron_backup_D_1_to_D_2 = BashOperator(
     task_id='copy_cron_backup_D_1_to_D_2',
     depends_on_past=False,
     bash_command='cp /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup_D-1.txt /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup_D-2.txt',
     dag=dag,
     run_as_user = 'daasuser'
)

copy_cron_backup_to_D_1 = BashOperator(
     task_id='copy_cron_backup_to_D_1',
     depends_on_past=False,
     bash_command='cp /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup.txt /mnt/beegfs_bsl2/tools/Rebhi/crontab_backup/cron_backup_D-1.txt',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_crontab_data = BashOperator(
     task_id='extract_crontab_data' ,
     depends_on_past=False,
     bash_command= backup_job_airflow,
     dag=dag,
     run_as_user = 'daasuser'
)

remove_cron_backup_D_3 >> copy_cron_backup_D_2_to_D_3 >> copy_cron_backup_D_1_to_D_2 >> copy_cron_backup_to_D_1 >> extract_crontab_data
