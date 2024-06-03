import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,date,timedelta
#import datetime

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
dag = DAG(dag_id='EDR_AGI_PAYMENT',
      	default_args=args,
        schedule_interval='0 7 * * *',
        catchup=True,
    	concurrency=1,
        max_active_runs=1
          )

phase_one = BashOperator(task_id='EDR_PAYMENT',bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p EDR_PAYMENT ',      dag=dag,
     run_as_user = 'daasuser')
phase_two = BashOperator(task_id = 'CB_PAYMENT_SUMMARY',bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p PAY_SUMMARY ',      dag=dag,
     run_as_user = 'daasuser')
phase_three = BashOperator(task_id='HVC_SUBS_DUE_LIST',bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p HVC',      dag=dag,
     run_as_user = 'daasuser')
phase_four = BashOperator(task_id='PAYMENT_RPT_ALL',bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p PAYMENT_ALL',      dag=dag,
     run_as_user = 'daasuser')
phase_onec = BashOperator(task_id='SERAGEING',bash_command='/nas/share05/ops/mtnops/for_seagingin_ingest.py -p SERAGEING',      dag=dag,
     run_as_user = 'daasuser')
phase_oneb = BashOperator(task_id='MSO_MNP_CAMPAIGN',bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p mso_bib_mnp_campaign -l 1 && hadoop fs -rm -r /user/hive/nigeria/mso_mnp_campaign/MSO_BIB_MNP_CAMPAIGN.txt',      dag=dag,run_as_user = 'daasuser')
phase_oneb_put = BashOperator(task_id='MSO_MNP_CAMPAIGN_put',bash_command='hadoop fs -put -f /nas/share05/ops/mtnops/MSO_BIB_MNP_CAMPAIGN.txt /user/hive/nigeria/mso_mnp_campaign/',      dag=dag,     run_as_user = 'daasuser')
phase_twob = BashOperator(task_id='MSO_VAS_SUBSC_REP',bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p VAS_SUBSC',dag=dag,run_as_user = 'daasuser')
join = DummyOperator(task_id='join',trigger_rule='all_success',      dag=dag, run_as_user = 'daasuser')
# Set the dependencies for both possibilities
phase_one >> join
phase_oneb >> phase_oneb_put >> join
phase_onec >> join
join >> phase_two >> phase_twob >>  phase_three >> phase_four
#>> phase_five >> phase_six
