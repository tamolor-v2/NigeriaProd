from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
 

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
    'email': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='GDS_REPORT_ADHOC',
    default_args=args,
    schedule_interval='0 8 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

t1 = BashOperator(
     task_id='GDS1' ,
     bash_command='bash /nas/share05/tools/GDS_REPORT/GDS_REPORT2_1.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

t2 = BashOperator(
     task_id='GDS2' ,
     bash_command='bash /nas/share05/tools/GDS_REPORT/GDS_REPORT2_2.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

t3 = BashOperator(
     task_id='GDS3' ,
     bash_command='bash /nas/share05/tools/GDS_REPORT/GDS_REPORT2_3.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

t4 = BashOperator(
     task_id='GDS4' ,
     bash_command='bash /nas/share05/tools/GDS_REPORT/GDS_REPORT2_4.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

t5 = BashOperator(
     task_id='GDS5' ,
     bash_command='bash /nas/share05/tools/GDS_REPORT/GDS_REPORT2_5.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


t1 >> t2 >> t3 >> t4 >> t5

