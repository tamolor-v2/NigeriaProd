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
    'email': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_failure': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'catchup':True,
    'depends_on_past': False,
    #'start_date': datetime(2020, 10, 9),
}



#date_param = (monthdelta(date.today(), -6)).strftime('%Y%m%d')


dag = DAG(
    dag_id='boost_dealer_commission_plan',
    default_args=args,
    schedule_interval='0 09 4,5 * *',
    description='run boost_dealer_commission_plan report and push the result into CSV file to client',    
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


input_validation = BashOperator(
    task_id='input_validation',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` input_validation',
    dag=dag,
    run_as_user='daasuser')
report_script = BashOperator(
    task_id='report_script',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` report_script',
    dag=dag,
    run_as_user='daasuser')
output_table_validation = BashOperator(
    task_id='output_table_validation',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` output_table_validation',
    dag=dag,
    run_as_user='daasuser')
extraction_csv = BashOperator(
    task_id='extraction_csv',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` extraction_csv',
    dag=dag,
    run_as_user='daasuser')      
check_if_empty_file = BashOperator(
    task_id='check_if_empty_file',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` check_if_empty_file',
    dag=dag,
    run_as_user='daasuser')  
split_file = BashOperator(
    task_id='split_file',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` split_file',
    dag=dag,
    run_as_user='daasuser')  
move_file_to_landing_path = BashOperator(
    task_id='move_file_to_landing_path',
    bash_command= 'bash /nas/share05/scripts/boost_integration/dealercommission/general_script.sh `date -d "-1 month -$(($(date +%d)-1)) days" "+%Y%m%d"` `date -d "-$(date +%d) days" "+%Y%m%d"` move_file_to_landing_path',
    dag=dag,
    run_as_user='daasuser')     
input_validation >> report_script >> output_table_validation >> extraction_csv >> check_if_empty_file >> split_file >> move_file_to_landing_path

