import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os


today = datetime.now() - timedelta(days=7)

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': today,
    'email': 'joshua.avbuere@mtn.com', 'naheem.bamidele@mtn.com'
    'email_on_failure': 'joshua.avbuere@mtn.com', 'naheem.bamidele@mtn.com'
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='REVPERBTS_CGI',
          default_args=args,
          schedule_interval='0 08 * * *'
#         schedule_interval=None

)


def end_dag():
    print('REVPERBTS CGI Report succesful.')

with dag:
    REVPERBTS_CGI_write = BashOperator(task_id='REVPERBTS_CGI',bash_command ='python3.6 /nas/share05/ops/mtnops/revbts_by_cgi.py -p ALL -l 3 -s `date --date="-1 days" +%Y-%m-%d`')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    REVPERBTS_CGI_write >> end_dag
