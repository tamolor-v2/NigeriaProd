import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
import time
from airflow.operators import PythonOperator, BashOperator

today = datetime.today()
run_date_2 = datetime.today() - timedelta(hours=2)
run_date_1 = datetime.today() - timedelta(hours=1)
run_1 = run_date_1.strftime('%Y%m%d')
run_2 = run_date_2.strftime('%Y%m%d')
run_hour1 = run_date_1.strftime('%H')
run_hour2 = run_date_2.strftime('%H')

def example():
    print("Hello")
    time.sleep(160)
    print("Word")

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['m.abdin@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG('Test_S',
    default_args=args,
#    schedule_interval=' 00-59/5 * * * * ',
    schedule_interval=None,
    concurrency=1,
    max_active_runs=1,
          )

test = BashOperator(
        task_id='test',
        bash_command="echo /nas/share05/ops/daily/wbs_extract_bk_2.sh {1} {2} {3} {0}".format(run_1,run_hour2,run_hour1,run_2),
        dag=dag,
        execution_timeout=timedelta(minutes=2),
        run_as_user='daasuser'
    )

move_files = PythonOperator(
    task_id='move_files',
    python_callable=example,
    dag=dag,
    execution_timeout=timedelta(minutes=2),
    run_as_user='daasuser'
)

exa = PythonOperator(
    task_id='exa',
    python_callable=example,
    dag=dag,
    execution_timeout=timedelta(minutes=2),
    run_as_user='daasuser'
)


	
test >> move_files >> exa
