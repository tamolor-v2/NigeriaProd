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

dag = DAG(
    dag_id='MOVE_TO_ECS_GROUP_1',
    default_args=args,
    schedule_interval='0 8 * * *',
    catchup=False,  
    concurrency=5,
    max_active_runs=5

)

START = BashOperator(
     task_id='start' ,
     bash_command='echo "start"	',
     run_as_user = 'daasuser',
     dag=dag,
)


CCN_CDR_Detail = BashOperator(
     task_id='ccn_cdr_detail' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CCN_CDR_Detail	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_GPRS_AC = BashOperator(
     task_id='cs5_ccn_gprs_ac' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_GPRS_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_GPRS_MA = BashOperator(
     task_id='cs5_ccn_gprs_ma' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_GPRS_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_VOICE_AC = BashOperator(
     task_id='cs5_ccn_voice_ac' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_VOICE_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_VOICE_MA = BashOperator(
     task_id='cs5_ccn_voice_ma' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_VOICE_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS6_CCN_CDR = BashOperator(
     task_id='cs6_ccn_cdr' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS6_CCN_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)

DPI_CDR_HISTORICAL = BashOperator(
     task_id='dpi_cdr_historical' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DPI_CDR_HISTORICAL	',
     run_as_user = 'daasuser',
     dag=dag,
)

DpiCdrUnpack = BashOperator(
     task_id='dpicdrunpack' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DpiCdrUnpack	',
     run_as_user = 'daasuser',
     dag=dag,
)

GGSN_CDR = BashOperator(
     task_id='ggsn_cdr' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh GGSN_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)

LEA_MAPPING_MSC_DAAS = BashOperator(
     task_id='lea_mapping_msc_daas' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh LEA_MAPPING_MSC_DAAS	',
     run_as_user = 'daasuser',
     dag=dag,
)


MSC_CDR = BashOperator(
     task_id='msc_cdr' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSC_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)

#MSC_DAAS = BashOperator(
#     task_id='msc_daas' ,
#     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSC_DAAS	',
#     run_as_user = 'daasuser',
#     dag=dag,
#)

WBS_PM_RATED_CDRS = BashOperator(
     task_id='wbs_pm_rated_cdrs' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh WBS_PM_RATED_CDRS	',
     run_as_user = 'daasuser',
     dag=dag,
)

END = BashOperator(
     task_id='end' ,
     bash_command='echo "end"	',
     run_as_user = 'daasuser',
     dag=dag,
)


START >> [CS5_CCN_GPRS_AC, CS5_CCN_GPRS_MA, CS5_CCN_VOICE_AC, CS5_CCN_VOICE_MA, CS6_CCN_CDR, DPI_CDR_HISTORICAL, DpiCdrUnpack, GGSN_CDR, MSC_CDR, WBS_PM_RATED_CDRS,CCN_CDR_Detail, LEA_MAPPING_MSC_DAAS] >> END

#LEA_MAPPING_MSC_DAAS ,MSC_DAAS

