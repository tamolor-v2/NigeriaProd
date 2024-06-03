from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

yesterday = datetime.today() - timedelta(days=1)

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': datetime(2020,11,30),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='ApiScriptiDailyGrp3',
    default_args=args,
    schedule_interval='0 6,12,18 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

ApiScriptiDailyGrp = BashOperator(
     task_id='ApiScriptiDailyGrp' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/ApiScriptiDailyGrp3.sh ',
     run_as_user='daasuser',
     dag=dag
)

ApiScriptiDailyMfs = BashOperator(
     task_id='ApiScriptiDailyMfs' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/ApiScriptiDailyGrp3_OneFeed.sh `date --date="-1 days" +%Y%m%d`  mfs_transaction_new ',
     run_as_user='daasuser',
     dag=dag
)

ApiScriptiDailyGrp >> ApiScriptiDailyMfs 
