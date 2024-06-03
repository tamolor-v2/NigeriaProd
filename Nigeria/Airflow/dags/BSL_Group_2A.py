import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators import BashOperator,PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator

list_feed = 'BUNDLE4U_GPRS,BUNDLE4U_VOICE,CS5_AIR_ADJ_MA,CS5_AIR_REFILL_MA,CS5_CCN_GPRS_MA,CS5_CCN_SMS_MA,CS5_CCN_VOICE_MA,CS5_SDP_ACC_ADJ_MA,DPI_CDR,MSC_CDR'

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = yesterday.strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=2)
hour_run = hour_run_date.strftime('%H')

phases = ['Extract_','Dedup2_','PCF_','faile_']
working_dir='/mnt/beegfs_bsl/Deployment/DEV/scripts/BslDriver'

dedup_command= Variable.get("dedup_command_1", deserialize_json=True)
dedup_command2= Variable.get("dedup_command_2", deserialize_json=True)
dedup_command_dpi_cdr= Variable.get("dedup_command_dpi_cdr", deserialize_json=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,12,3),
    'email': ['m.abdin@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}



dag = DAG('BSL_Live_feeds_Dedup_Daily', default_args=default_args, catchup=False, schedule_interval= "0 19 * * *")


Dedup_bundle4u_gprs = BashOperator(
    task_id='Dedup_bundle4u_gprs',
    bash_command= dedup_command.format(run_date, 'bundle4u_gprs', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_bundle4u_voice = BashOperator(
    task_id='Dedup_bundle4u_voice',
    bash_command= dedup_command.format(run_date, 'bundle4u_voice', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_cs5_air_adj_ma = BashOperator(
    task_id='Dedup_cs5_air_adj_ma',
    bash_command= dedup_command.format(run_date, 'cs5_air_adj_ma', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_cs5_air_refill_ma = BashOperator(
    task_id='Dedup_cs5_air_refill_ma',
    bash_command= dedup_command.format(run_date, 'cs5_air_refill_ma', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_cs5_ccn_gprs_ma = BashOperator(
    task_id='Dedup_cs5_ccn_gprs_ma',
    bash_command= dedup_command.format(run_date, 'cs5_ccn_gprs_ma', 'false'),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_cs5_ccn_gprs_ma >> Dedup_cs5_air_refill_ma >> Dedup_cs5_air_adj_ma >> Dedup_bundle4u_voice >> Dedup_bundle4u_gprs

Dedup_cs5_ccn_sms_ma = BashOperator(
    task_id='Dedup_cs5_ccn_sms_ma',
    bash_command= dedup_command2.format(run_date, 'cs5_ccn_sms_ma', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_cs5_ccn_voice_ma = BashOperator(
    task_id='Dedup_cs5_ccn_voice_ma',
    bash_command= dedup_command2.format(run_date, 'cs5_ccn_voice_ma', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='Dedup_cs5_sdp_acc_adj_ma',
    bash_command= dedup_command2.format(run_date, 'cs5_sdp_acc_adj_ma', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_dpi_cdr = BashOperator(
    task_id='Dedup_dpi_cdr',
    bash_command= dedup_command_dpi_cdr.format(run_date, 'dpi_cdr_00,dpi_cdr_01,dpi_cdr_02,dpi_cdr_03,dpi_cdr_04,dpi_cdr_05,dpi_cdr_06,dpi_cdr_07,dpi_cdr_08,dpi_cdr_09,dpi_cdr_10,dpi_cdr_11,dpi_cdr_12,dpi_cdr_13,dpi_cdr_14,dpi_cdr_15,dpi_cdr_16,dpi_cdr_17,dpi_cdr_18,dpi_cdr_19,dpi_cdr_20,dpi_cdr_21,dpi_cdr_22,dpi_cdr_23', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


Dedup_msc_cdr = BashOperator(
    task_id='Dedup_msc_cdr',
    bash_command= dedup_command2.format(run_date, 'msc_cdr', 'false'),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_msc_cdr >> Dedup_dpi_cdr >> Dedup_cs5_sdp_acc_adj_ma >> Dedup_cs5_ccn_voice_ma >> Dedup_cs5_ccn_sms_ma
