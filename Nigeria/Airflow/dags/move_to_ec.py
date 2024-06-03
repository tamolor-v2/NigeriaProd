from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import calendar
from datetime import date


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}


def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, calendar.monthrange(y, m)[1])
    return date.replace(day=d,month=m, year=y)

date_param = (monthdelta(date.today(), -6)).strftime('%Y%m%d')


dag = DAG(
    dag_id='MOVE_TO_EC',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)



MSC_DAAS = BashOperator(
     task_id='MSC_DAAS' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh MSC_DAAS {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_GPRS_AC = BashOperator(
     task_id='CS5_CCN_GPRS_AC' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_GPRS_AC {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_GPRS_DA = BashOperator(
     task_id='CS5_CCN_GPRS_DA' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_GPRS_DA {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_SMS_AC = BashOperator(
     task_id='CS5_CCN_SMS_AC' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_SMS_AC {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)
CS5_CCN_SMS_DA = BashOperator(
     task_id='CS5_CCN_SMS_DA' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_SMS_DA {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)
CS5_CCN_VOICE_AC = BashOperator(
     task_id='CS5_CCN_VOICE_AC' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_VOICE_AC {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_VOICE_DA = BashOperator(
     task_id='CS5_CCN_VOICE_DA' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_VOICE_DA {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)
CS5_CCN_GPRS_MA = BashOperator(
     task_id='CS5_CCN_GPRS_MA' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh CS5_CCN_GPRS_MA {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

LEA_MAPPING_MSC_DAAS = BashOperator(
     task_id='LEA_MAPPING_MSC_DAAS' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run.sh LEA_MAPPING_MSC_DAAS {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

DPI_CDR_new = BashOperator(
     task_id='DPI_CDR_new' ,
     bash_command='bash /mnt/beegfs_api/Abdallah/Move_EC_run_DPI.sh DPI_CDR_new {0}'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


MSC_DAAS >> CS5_CCN_GPRS_AC >> CS5_CCN_GPRS_DA >> CS5_CCN_SMS_AC >> CS5_CCN_SMS_DA >> CS5_CCN_VOICE_AC >> CS5_CCN_VOICE_DA >> DPI_CDR_new >> CS5_CCN_GPRS_MA >> LEA_MAPPING_MSC_DAAS 
