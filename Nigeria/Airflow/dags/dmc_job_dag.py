import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='dmc_dimension_task',
          default_args=args,
          schedule_interval='0 6 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def end_dag():
    print('dmc dimension refreshed succesfully for {0}'.format(date_param))

with dag:
    kick_off_tac_task = BashOperator(task_id='kick_off_tac_task',bash_command='python3.6 /nas/share05/ops/mtnops/dim_tac_dmc.py {0} '.format(date_param))

    DMC_SOURCE = BashOperator(task_id = 'DMC_SOURCE',bash_command='python3.6 /nas/share05/ops/mtnops/dim_dmc_msisdn.py {0} "DMC" '.format(date_param))

    GPRS_SOURCE = BashOperator(task_id='GPRS_SOURCE',bash_command='python3.6 /nas/share05/ops/mtnops/dim_dmc_msisdn.py {0} "GPRS"'.format(date_param))

    VOICE_SOURCE = BashOperator(task_id = 'VOICE_SOURCE',bash_command='python3.6 /nas/share05/ops/mtnops/dim_dmc_msisdn.py {0} "VOICE" '.format(date_param))

    SMS_SOURCE = BashOperator(task_id = 'SMS_SOURCE',bash_command='python3.6 /nas/share05/ops/mtnops/dim_dmc_msisdn.py {0} "SMS"'.format(date_param))

    join = DummyOperator(task_id='join',trigger_rule='all_success')

    DIM_DMC_MSISDN = BashOperator(task_id = 'DIM_DMC_MSISDN',bash_command='python3.6 /nas/share05/ops/mtnops/dim_dmc_msisdn.py {0} "SUMM"'.format(date_param))

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    kick_off_tac_task >> DMC_SOURCE
    kick_off_tac_task >> GPRS_SOURCE
    kick_off_tac_task >> VOICE_SOURCE
    kick_off_tac_task >> SMS_SOURCE
    SMS_SOURCE >> join
    DMC_SOURCE >> join
    GPRS_SOURCE >> join
    VOICE_SOURCE >> join
    join >> DIM_DMC_MSISDN >> end_dag

