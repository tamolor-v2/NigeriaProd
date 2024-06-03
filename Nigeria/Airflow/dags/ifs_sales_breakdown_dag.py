import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(2),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=3),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='ifs_sales_breakkdown',
          default_args=args,
          schedule_interval='0 8 * * *')

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

def first_day_of_month(any_day):
    first_day = any_day.replace(day=1) 
    return first_day

date_param2 = datetime.strftime(last_day_of_month(datetime.now()),'%Y%m%d')
date_param = datetime.strftime(first_day_of_month(datetime.now()),'%Y%m%d')

def end_dag():
    print('ifs sales breakkdown report refreshed succesfully for {0} to {1}'.format(date_param,date_param2))

with dag:    
    kick_off_ifs_sales_brkdwn = BashOperator(task_id='ifs_sales_extract',bash_command='python3.6 /nas/share05/dataOps_prod/ifs_sales_breakdown/ifs_sales_breakdown.py {0} {1} '.format(date_param,date_param2), run_as_user = 'daasuser')

    ifs_sales_brkdwn = BashOperator(task_id='ifs_sales_brkdwn',bash_command='bash /nas/share05/dataOps_prod/ifs_sales_breakdown/Ingest_ifs_sales_bkdown.sh {0} {1} '.format(date_param,date_param2), run_as_user = 'daasuser')

    ayoba_traffic = BashOperator(task_id='ayoba_traffic',bash_command='bash /nas/share05/dataOps_dev/ayoba_traffic/ayoba_sms_traffic.sh `date --date="-1 days" +%Y%m%d` ', run_as_user = 'daasuser')

    ayoba_a2p_traffic = BashOperator(task_id='ayoba_a2p_traffic',bash_command='bash /nas/share05/dataOps_dev/ayoba_traffic/ayoba_A2p_sms_traffic.sh `date --date="-1 days" +%Y%m%d` ', run_as_user = 'daasuser')

    ayoba_error_code = BashOperator(task_id='ayoba_error_code',bash_command='bash /nas/share05/dataOps_dev/ayoba_traffic/ayoba_errorcnt.sh `date --date="-1 days" +%Y%m%d` ', run_as_user = 'daasuser')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag,run_as_user = 'daasuser')


    # Set the dependencies for both possibilities
    kick_off_ifs_sales_brkdwn >> ifs_sales_brkdwn >> ayoba_traffic >> ayoba_a2p_traffic >> ayoba_error_code >> end_dag

