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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')




dag = DAG(
    dag_id='CS18_DEDUP',
    default_args=args,
    schedule_interval='0 20 * * *',
    description='cs18 D-2 DEDUP run',
    catchup=False,
    concurrency=10,
    max_active_runs=10
)

CS6_CCN_CDR_SMS = BashOperator(
     task_id='CS6_CCN_CDR_SMS' ,
     bash_command='bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f CS6_CCN_CDR_SMS -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_m1004_$(date +%Y%m%d%H%M%S)ev.txt	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

CS6_CCN_CDR_GPRS = BashOperator(
     task_id='CS6_CCN_CDR_GPRS' ,
     bash_command='bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f CS6_CCN_CDR_GPRS -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_m1004_$(date +%Y%m%d%H%M%S)ev.txt	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


CS6_CCN_CDR_VOICE = BashOperator(
     task_id='CS6_CCN_CDR_VOICE' ,
     bash_command='bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f CS6_CCN_CDR_VOICE -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_m1004_$(date +%Y%m%d%H%M%S)ev.txt	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


CS6_CCN_CDR_VAS = BashOperator(
     task_id='CS6_CCN_CDR_VAS' ,
     bash_command='bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f CS6_CCN_CDR_VAS -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_m1004_$(date +%Y%m%d%H%M%S)ev.txt	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

CS6_AIR_CDR = BashOperator(
     task_id='CS6_AIR_CDR' ,
     bash_command='bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f CS6_AIR_CDR -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_m1004_$(date +%Y%m%d%H%M%S)ev.txt	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

CS6_SDP_CDR = BashOperator(
     task_id='CS6_SDP_CDR' ,
     bash_command='bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f CS6_SDP_CDR -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_m1004_$(date +%Y%m%d%H%M%S)ev.txt	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


CS6_CCN_CDR_SMS >> CS6_CCN_CDR_GPRS >> CS6_CCN_CDR_VOICE >> CS6_CCN_CDR_VAS >> CS6_AIR_CDR >> CS6_SDP_CDR

