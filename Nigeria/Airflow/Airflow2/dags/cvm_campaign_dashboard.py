from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen
args  = {
  'owner': 'MTN Nigeria CVM',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(1),
  'email': ['support@ligadata.com','m.shekha@ligadata.com','hendre@ligadata.com','o.olanipekun@ligadata.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'email_on_success':True,      
  'retries': 5,
  'retry_delay': timedelta(minutes = 15),
  'catchup':False,
  #'queue': 'bash_queue',
  #'pool': 'backfill',
  #'priority_weight': 10,
  #'end_date': datetime(2016, 1, 1),
  #'wait_for_downstream': False,
  #'dag': dag,
  #'sla': timedelta(hours = 2),
  #'execution_timeout': timedelta(seconds = 300),
  #'on_failure_callback': some_function,
  #'on_success_callback': some_other_function,
  #'on_retry_callback': another_function,
}
dag = DAG(
    'cvm_campaign_dashboard',
    default_args=args,
    description='update campaign dashboard with all needed info for the campaigns ended at d-1',
    schedule_interval='30 5 * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=1
    )




#Run Library, WB Broadcast, DB HUB   
#update library and hub tables -> cvmcampaign_library, cvmcampaign_wb_broadcast, cvmcampaign_db_hub d-8 -> d-1
#cvmcampaign_hub = BashOperator(
#    task_id='cvmcampaign_hub',
#    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-8 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_hub daily 8999 "" 1 ',
#    dag=dag,
#    run_as_user='daasuser')


# note back log catchup on 20210121 by add 5 days back to each task 
cvmcampaign_library = BashOperator(
    task_id='cvmcampaign_library',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-8 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_library daily 8999 "" 1 ',
    dag=dag,
    run_as_user='daasuser')
    
cvmcampaign_wb_broadcast = BashOperator(
    task_id='cvmcampaign_wb_broadcast',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-8 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_wb_broadcast daily 8999 "" 1 ',
    dag=dag,
    run_as_user='daasuser')

cvmcampaign_db_hub = BashOperator(
    task_id='cvmcampaign_db_hub',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-8 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_db_hub daily 8999 "" 1 ',
    dag=dag,
    run_as_user='daasuser')

#update hub tables to insert late arrival scan (30 days back in write back)  cvmcampaign_wb_broadcast, cvmcampaign_db_hub d-29 -> d-1
cvmcampaign_wb_broadcast_la = BashOperator(
    task_id='cvmcampaign_wb_broadcast_la',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_wb_broadcast_la daily 8999 "" 1 ',
    dag=dag,
    run_as_user='daasuser')

cvmcampaign_db_hub_la = BashOperator(
task_id='cvmcampaign_db_hub_la',
bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_db_hub_la daily 8999 "" 1 ',
dag=dag,
run_as_user='daasuser')

#check sub for d-1
sub_validation = BashOperator(
     task_id='sub_validation' ,
     bash_command='bash /nas/share05/scripts/cvm/campaign_dm/sub_check.sh `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
)

#update measure table cvmcampaign_measures d-3 -> d-1
cvmcampaign_measures = BashOperator(
    task_id='cvmcampaign_measures',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_measures daily 8999  "" 1',
    dag=dag,
    run_as_user='daasuser')

#Validation that measures is done and healthy
measures_validation = BashOperator(
    task_id='measures_validation',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/validation.sh  `date --date="-1 days" +%Y%m%d`  `date --date="-2 days" +%Y%m%d` `date --date="-3 days" +%Y%m%d` `date --date="-4 days" +%Y%m%d`',
    dag=dag,
    run_as_user='daasuser')

#Validation that measures is done and healthy -task 2
measures_validation_2 = BashOperator(
    task_id='measures_validation_2',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/validation.sh  `date --date="-1 days" +%Y%m%d`  `date --date="-2 days" +%Y%m%d` `date --date="-3 days" +%Y%m%d` `date --date="-4 days" +%Y%m%d`',
    dag=dag,
    run_as_user='daasuser')


#Validation that hub in the other thread is done 
#hub_validation = BashOperator(
#    task_id='hub_validation',
#    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/hub_check.sh  `date --date="-1 days" +%Y%m%d`  ',
#    dag=dag,
#    run_as_user='daasuser')


#--Run Normal List Campaigns, Clean >>(SUBS, PREPOST, SC, UNION). on Threads. The date you pass is the end date of the campaigns.   
cvmcampaign_daily_campaigns = BashOperator(
    task_id='cvmcampaign_daily_campaigns',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_daily_campaigns daily 8099 "" 10',
    dag=dag,
    run_as_user='daasuser')

# for now we run fixing on d-1 once backlog is ok we will run late arrival for 30 days
cvmcampaign_daily_campaigns_anomalies = BashOperator(
    task_id='cvmcampaign_daily_campaigns_anomalies',
    bash_command= 'bash /nas/share05/scripts/cvm/campaign_dm/run_cvmcampaign_p.sh  `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` cvmcampaign_daily_campaigns_la daily 8099 "" 10',
    dag=dag,
    run_as_user='daasuser')

#cvmcampaign_library >> cvmcampaign_wb_broadcast >> cvmcampaign_db_hub >> cvmcampaign_wb_broadcast_la >> cvmcampaign_db_hub_la >> measures_validation >>  cvmcampaign_daily_campaigns >> cvmcampaign_daily_campaigns_la

# send email   
send_email = BashOperator(
    task_id='send_email',
    bash_command= ' bash /nas/share05/scripts/cvm/campaign_dm/email/send_email.sh "Library Load" 1 ',
    dag=dag,
    run_as_user='daasuser')
# send email   
send_email_2 = BashOperator(
    task_id='send_email_2',
    bash_command= ' bash /nas/share05/scripts/cvm/campaign_dm/email/send_email.sh "D-1 Campaigns Load" 1 ',
    dag=dag,
    run_as_user='daasuser')
send_email_3 = BashOperator(
    task_id='send_email_3',
    bash_command= ' bash /nas/share05/scripts/cvm/campaign_dm/email/send_email.sh "Anomalies and Late Arrival Campaigns Load" 1',
    dag=dag,
    run_as_user='daasuser')

cvmcampaign_library  >> cvmcampaign_wb_broadcast >> cvmcampaign_wb_broadcast_la >>send_email
cvmcampaign_library >> cvmcampaign_db_hub >>  cvmcampaign_db_hub_la >>send_email

# we need to add hub_validation validation here 
#sub_validation >> cvmcampaign_measures >> measures_validation >> hub_validation >> cvmcampaign_daily_campaigns  >> send_email_2 >> measures_validation_2 >> cvmcampaign_daily_campaigns_anomalies >> send_email_3
sub_validation >> cvmcampaign_measures >> measures_validation >> cvmcampaign_daily_campaigns  >> send_email_2 >> measures_validation_2 >> cvmcampaign_daily_campaigns_anomalies >> send_email_3

