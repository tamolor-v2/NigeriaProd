import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,timedelta
#import datetime

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': seven_days_ago,
    'email': ['olorunsegun.adeniyi@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['olorunsegun.adeniyi@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

# instantiate dag
dag = DAG(dag_id='ab',
      	default_args=args,
        schedule_interval='33 3 * * *',
        catchup=True,
    	concurrency=3,
        max_active_runs=3
          )
one = BashOperator(task_id='stage1',bash_command='presto --server 10.1.197.145:8668 --catalog hive5 --schema nigeria --output-format CSV_HEADER --debug -f /nas/share05/Eben/ab.sql ',dag=dag,run_as_user = 'daasuser',retry_delay=timedelta(minutes=1),retries=2)
