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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
date_param_1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
#date_param_1_1 = (datetime.now() - timedelta(days=6)).strftime('%Y%m%d')
 
 
dag = DAG(
    dag_id='SEGMENT_5BY5_LATE_RUN',
    default_args=args,
    schedule_interval='56 16 * * *',
    description='DO NOT TURN OFF',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)


SEGMENT5B5_DEV = BashOperator(
     task_id='SEGMENT5B5_DEV' ,
     bash_command='bash /nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {1} segment5b5_dev_main daily 8668 > /dev/null &  '.format(date_param_1,date_param),
     dag=dag,
)

SEGMENT5B5_REV = BashOperator(
     task_id='SEGMENT5B5_REV' ,
     bash_command='bash /nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {1} segment5b5_rev_main daily 8668 > /dev/null &  '.format(date_param_1,date_param),
     dag=dag,
)

SEGMENT5B5_USG = BashOperator(
     task_id='SEGMENT5B5_USG' ,
     bash_command='bash /nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {1} segment5b5_usg_main daily 8668 > /dev/null &	'.format(date_param_1,date_param),
     dag=dag,
)

SEGMENT5B5_RCH = BashOperator(
     task_id='SEGMENT5B5_RCH' ,
     bash_command='bash /nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {1} segment5b5_rch_main daily 8999 > /dev/null &  '.format(date_param_1,date_param),
     dag=dag,
)

JOIN = BashOperator(
     task_id='join' ,
     bash_command='echo join',
     dag=dag,
)

SEGMENT5B5_SUB = BashOperator(
     task_id='SEGMENT5B5_SUB' ,
     bash_command='bash /nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {1} segment5b5_sub_main daily 8668 > /dev/null &  '.format(date_param_1,date_param),
     dag=dag,
)

SEGMENT5B5_MON = BashOperator(
     task_id='SEGMENT5B5_MON' ,
     bash_command='bash /nas/share05/scripts/segment5b5/monthly_check.sh {0} {1} '.format(date_param_1,date_param)  ,
     dag=dag,
)

#update measure table cvmcampaign_measures d-3 -> d-1
cvmcampaign_measures = BashOperator(
    task_id='cvmcampaign_measures',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign.sh {0} {1} cvmcampaign_measures daily 8999  '.format(date_param_1,date_param),
    dag=dag,
    run_as_user='daasuser')

[SEGMENT5B5_RCH >> SEGMENT5B5_USG, SEGMENT5B5_DEV >> SEGMENT5B5_REV] >> JOIN >> SEGMENT5B5_SUB >> [SEGMENT5B5_MON,cvmcampaign_measures]
