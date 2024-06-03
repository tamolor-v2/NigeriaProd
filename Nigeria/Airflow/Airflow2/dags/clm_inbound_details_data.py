import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta, date
default_args = {
'owner': 'CLM',
    'depends_on_past':False,
'start_date': airflow.utils.dates.days_ago(1),
'email': ['support@ligadata.com','m.shekha@ligadata.com'],
'email_on_failure': ['support@ligadata.com','m.shekha@ligadata.com'],
'email_on_failure': True,
'email_on_retry': True,
'email_on_success':True,  
'retries': 5,
'retry_delay': timedelta(minutes=3),
'catchup':False,
}



dag = DAG(
    'clm_inbound_details_data',
    default_args=default_args,
    description='run clm inbound details data script',
    schedule_interval='0 9 * * *') 
    
report_script_clm_run_since_last_day = BashOperator(
    task_id='report_script_clm_run_since_last_day',
    bash_command= 'bash /nas/share05/scripts/clm_cvm/clm_inbound_details/clm_run_since_last_day.sh  `date --date="-1 days" +%Y%m%d`',
    dag=dag,
    run_as_user='daasuser')

clm_wbo_enhancement_run_since_last_day = BashOperator(
    task_id='clm_wbo_enhancement_run_since_last_day',
    bash_command= 'bash /nas/share05/scripts/clm_cvm/clm_wbo_enhancement/clm_wbo_enhancement_run_since_last_day.sh  `date --date="-1 days" +%Y%m%d`',
    dag=dag,
    run_as_user='daasuser')

report_script_clm_run_since_last_day 
clm_wbo_enhancement_run_since_last_day