from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


dag = DAG(
    dag_id='EngineRoom_Insertion_yest',
    default_args=args,
    schedule_interval='30 7 * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)
Insert_MFS_REGISTRATIONS_yest = BashOperator(
     task_id='Insert_MFS_REGISTRATIONS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MFS_REGISTRATIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_ACQUISITION_yest = BashOperator(
     task_id='Insert_MFS_ACQUISITION_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MFS_ACQUISITION.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_EVD_TRANSACTIONS_yest = BashOperator(
     task_id='Insert_EVD_TRANSACTIONS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_EVD_TRANSACTIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_EVD_STOCK_LEVEL_yest = BashOperator(
     task_id='Insert_EVD_STOCK_LEVEL_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_EVD_STOCK_LEVEL.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_RECHARGE_yest = BashOperator(
     task_id='Insert_CDR_RECHARGE_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_CDR_RECHARGE.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_SUBSCRIPTION_yest = BashOperator(
     task_id='Insert_CDR_SUBSCRIPTION_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_CDR_SUBSCRIPTION.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_ACTIVE_SUBSCRIBERS_yest = BashOperator(
     task_id='Insert_MFS_ACTIVE_SUBSCRIBERS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MFS_ACTIVE_SUBSCRIBERS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_STOCK_LEVEL_yest = BashOperator(
     task_id='Insert_MFS_STOCK_LEVEL_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MFS_STOCK_LEVEL.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_SIM_TRANSACTIONS_yest = BashOperator(
     task_id='Insert_SIM_TRANSACTIONS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_SIM_TRANSACTIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_ACCOUNTS_yest = BashOperator(
     task_id='Insert_MFS_ACCOUNTS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MFS_ACCOUNTS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_DATA_yest = BashOperator(
     task_id='Insert_CDR_DATA_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_CDR_DATA.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_VOICE_yest = BashOperator(
     task_id='Insert_CDR_VOICE_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_CDR_VOICE.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_SMS_yest = BashOperator(
     task_id='Insert_CDR_SMS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_CDR_SMS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MSISDN_DISTRIBUTOR_yest = BashOperator(
     task_id='Insert_MSISDN_DISTRIBUTOR_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MSISDN_DISTRIBUTOR.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_POS_MSISDN_yest = BashOperator(
     task_id='Insert_POS_MSISDN_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_POS_MSISDN.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_POS_GEO_PLACES_yest = BashOperator(
     task_id='Insert_POS_GEO_PLACES_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_POS_GEO_PLACES.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_PHYSICAL_RECHARGES_yest = BashOperator(
     task_id='Insert_PHYSICAL_RECHARGES_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_PHYSICAL_RECHARGES.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_TRANSACTIONS_yest = BashOperator(
     task_id='Insert_MFS_TRANSACTIONS_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_MFS_TRANSACTIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_BUNDLE_SPLIT_yest = BashOperator(
     task_id='Insert_BUNDLE_SPLIT_yest' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_yest/Insert_BUNDLE_SPLIT.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
