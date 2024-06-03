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
    'catchup':True,
}

dag = DAG(
    dag_id='BULK_SMS', 
    default_args=args,
    schedule_interval='0 7 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

bulk_sms = BashOperator(
     task_id='bulk_sms' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/bulk_sms.py `date --date="-0 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

eyeballing_recycle = BashOperator(
     task_id='eyeballing_recycle' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/eyeballing_recycle.py `date --date="-0 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

smart_user = BashOperator(
     task_id='smart_user' ,
     bash_command='perl /nas/share05/ops/daily/SmartApp_Report.pl  `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

data_recon = BashOperator(
     task_id='data_recon' ,
     bash_command='bash /nas/share05/ops/daily/recon_data_bundle_activation.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

data_recon >> bulk_sms >> eyeballing_recycle >> smart_user
