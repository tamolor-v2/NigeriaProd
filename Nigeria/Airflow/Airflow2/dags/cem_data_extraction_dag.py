import airflow
from airflow import DAG,exceptions,AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

date_param = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': True,
    'retries': 12,
    'retry_delay': timedelta(minutes=15),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='CEM_DATA_EXTRACTION',
          default_args=args,
          schedule_interval='35 8 * * *')

def check_5by5(date_p=date_param):
        presto_sql = 'select count(*) from nigeria.segment5b5_mon where tbl_dt = {0}'.format(date_p)
        CLI_CMD="""presto --server 10.1.197.146:8099 --catalog hive5 --schema flare_8 --output-format TSV --client-tags oladimo --execute '{0}' > /nas/share05/dataOps_dev/cem_extract/new_5by5.txt""".format(presto_sql)
        print(CLI_CMD)
        os.system(CLI_CMD)
        file = open('/nas/share05/dataOps_dev/cem_extract/new_5by5.txt', "r")
        count = file.read()
        file.close()
        if int(count) < 60000000:
        	raise AirflowException('Failed on 5by5 D-1...retry would happen in 15mins.')



def init_dag():
    print('CEM Data extracted initializing...')

def end_dag():
    print('CEM Data logs extracted succesfully')

with dag:
    extract_cem_data = BashOperator(task_id='extract_cem_data',bash_command='python3.6 /nas/share05/dataOps_dev/cem_extract/CEM_extract_v2.py `date --date="-1 days" +%Y%m%d` ',dag=dag,run_as_user = 'daasuser')

    check_5by5 = PythonOperator(task_id='check_5by5',python_callable=check_5by5,run_as_user = 'daasuser')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)

    init_dag = PythonOperator(task_id='init_dag',python_callable=init_dag)


    # Set the dependencies for both possibilities
    init_dag >> check_5by5 >> extract_cem_data >> end_dag

