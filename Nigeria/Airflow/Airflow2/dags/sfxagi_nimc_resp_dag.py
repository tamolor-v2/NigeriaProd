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
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=2),
    'catchup':False,
}

def extract_from_agi(date_p=date_param):
	os.system('bash /nas/share05/dataOps_dev/sfxagi_nimc_response/extract_from_agi.sh  `date --date"=-1 days" +%Y%m%d`')
	presto_sql = 'select count(*) from dataops_dev.nimc_response_agi where tbl_dt = {0}'.format(date_p)
	CLI_CMD="""presto --server 10.1.197.146:8099 --catalog hive5 --schema flare_8 --output-format TSV --client-tags oladimo --execute '{0}' > /nas/share05/dataOps_dev/sfxagi_nimc_response/agi_rec.txt""".format(presto_sql)
	os.system(CLI_CMD)
	file = open('/nas/share05/dataOps_dev/sfxagi_nimc_response/agi_rec.txt', "r")
	count = file.read()
	file.close()
	if int(count) < 1:
		os.system('rm -f /nas/share05/dataOps_dev/sfxagi_nimc_response/from_agility_*.txt')
		raise AirflowException('Failed on agility extraction D-1...retry would happen in 2mmins.')

def extract_from_sfx(date_p=date_param):
	os.system('bash /nas/share05/dataOps_dev/sfxagi_nimc_response/extract_from_sfx.sh  `date --date"=-1 days" +%Y%m%d`')
	presto_sql = 'select count(*) from dataops_dev.nimc_response_sfx where tbl_dt = {0}'.format(date_p)
	CLI_CMD="""presto --server 10.1.197.146:8099 --catalog hive5 --schema flare_8 --output-format TSV --client-tags oladimo --execute '{0}' > /nas/share05/dataOps_dev/sfxagi_nimc_response/sfx_rec.txt""".format(presto_sql)
	os.system(CLI_CMD)
	file = open('/nas/share05/dataOps_dev/sfxagi_nimc_response/sfx_rec.txt', "r")
	count = file.read()
	file.close()
	if int(count) < 1:
		os.system('rm -f /nas/share05/dataOps_dev/sfxagi_nimc_response/from_seamfix_*.txt')
		raise AirflowException('Failed on sfx extraction D-1...retry would happen in 2mins.')
# instantiate dag
dag = DAG(dag_id='SIMREG_vs_NIN_Verification',
          default_args=args,
          schedule_interval='10 6 * * *')
def init_dag():
    print('SIMREG vs NIN Verification report initializing...')

def end_dag():
    print('SIMREG vs NIN Verification report succesfully')

with dag:
    extract_from_agi = PythonOperator(task_id='extract_from_agi',python_callable=extract_from_agi ,dag=dag,run_as_user = 'daasuser')

    extract_from_sfx = PythonOperator(task_id='extract_from_sfx',python_callable=extract_from_sfx ,dag=dag,run_as_user = 'daasuser')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)

    init_dag = PythonOperator(task_id='init_dag',python_callable=init_dag)


    # Set the dependencies for both possibilities
    init_dag >> [extract_from_agi,extract_from_sfx] >> end_dag

