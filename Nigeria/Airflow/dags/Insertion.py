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
    'email': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','a.qayyas@ligadata.com','yulbeh@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG(
    dag_id='EngineRoom_Insertion',
    default_args=args,
    schedule_interval='0 5 * * * ',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)
Insert_MFS_REGISTRATIONS = BashOperator(
     task_id='Insert_MFS_REGISTRATIONS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MFS_REGISTRATIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_ACQUISITION = BashOperator(
     task_id='Insert_MFS_ACQUISITION' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MFS_ACQUISITION.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_EVD_TRANSACTIONS = BashOperator(
     task_id='Insert_EVD_TRANSACTIONS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_EVD_TRANSACTIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_EVD_STOCK_LEVEL = BashOperator(
     task_id='Insert_EVD_STOCK_LEVEL' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_EVD_STOCK_LEVEL.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_RECHARGE = BashOperator(
     task_id='Insert_CDR_RECHARGE' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_CDR_RECHARGE.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_SUBSCRIPTION = BashOperator(
     task_id='Insert_CDR_SUBSCRIPTION' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_CDR_SUBSCRIPTION.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_ACTIVE_SUBSCRIBERS = BashOperator(
     task_id='Insert_MFS_ACTIVE_SUBSCRIBERS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MFS_ACTIVE_SUBSCRIBERS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_STOCK_LEVEL = BashOperator(
     task_id='Insert_MFS_STOCK_LEVEL' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MFS_STOCK_LEVEL.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_SIM_TRANSACTIONS = BashOperator(
     task_id='Insert_SIM_TRANSACTIONS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_SIM_TRANSACTIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_ACCOUNTS = BashOperator(
     task_id='Insert_MFS_ACCOUNTS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MFS_ACCOUNTS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_DATA = BashOperator(
     task_id='Insert_CDR_DATA' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_CDR_DATA.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_VOICE = BashOperator(
     task_id='Insert_CDR_VOICE' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_CDR_VOICE.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_CDR_SMS = BashOperator(
     task_id='Insert_CDR_SMS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_CDR_SMS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MSISDN_DISTRIBUTOR = BashOperator(
     task_id='Insert_MSISDN_DISTRIBUTOR' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MSISDN_DISTRIBUTOR.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_POS_MSISDN = BashOperator(
     task_id='Insert_POS_MSISDN' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_POS_MSISDN.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_POS_GEO_PLACES = BashOperator(
     task_id='Insert_POS_GEO_PLACES' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_POS_GEO_PLACES.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_PHYSICAL_RECHARGES = BashOperator(
     task_id='Insert_PHYSICAL_RECHARGES' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_PHYSICAL_RECHARGES.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_MFS_TRANSACTIONS = BashOperator(
     task_id='Insert_MFS_TRANSACTIONS' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_MFS_TRANSACTIONS.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)
Insert_BUNDLE_SPLIT = BashOperator(
     task_id='Insert_BUNDLE_SPLIT' ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Engine_Room/Insertion_scripts/Insert_BUNDLE_SPLIT.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)



