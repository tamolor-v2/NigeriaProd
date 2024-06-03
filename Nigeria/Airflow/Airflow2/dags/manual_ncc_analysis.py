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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='NCC_ANALYSIS',
    default_args=args,
    schedule_interval='0 12 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

SRC_SIMREG_BASE = BashOperator(
     task_id='SRC_SIMREG_BASE' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/src_simreg_base.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)

seamfix_sms_act = BashOperator(
     task_id='seamfix_sms_act' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/seamfix_sms_act.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)

prov_recycle = BashOperator(
     task_id='prov_recycle' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/prov_recycle.py ',
     dag=dag,
     run_as_user = 'daasuser',
)

BASE_vs_5BY5 = BashOperator(
     task_id='BASE_vs_5BY5' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/BASE_vs_5BY5.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)

base_vs_subs_snapd = BashOperator(
     task_id='base_vs_subs_snapd' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/base_vs_subs_snapd.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)

barring_offerlist = BashOperator(
     task_id='barring_offerlist' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/barring_offerlist.py `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser',
)

final_analysis = BashOperator(
     task_id='final_analysis' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/final_analysis.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)

final_analysis_old = BashOperator(
     task_id='final_analysis_old' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/final_analysis_old.py  `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)


simreg_analysis_revenue = BashOperator(
     task_id='simreg_analysis_revenue' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/simreg_analysis_revenue.py `date --date="-1 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser',
)



SRC_SIMREG_BASE >> seamfix_sms_act >> [prov_recycle,BASE_vs_5BY5,base_vs_subs_snapd,barring_offerlist] >> final_analysis >> final_analysis_old >> simreg_analysis_revenue
