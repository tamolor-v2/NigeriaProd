import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(0),
                                  datetime.min.time())

today = datetime.now()
first = today.replace(day=1)
first_day=first.strftime("%Y%m%d")
#first day of the new month
lastMonth = first - timedelta(days=1)
day1 = lastMonth.strftime("%Y%m%d")
#last day of the previous Month
mnth_key = day1[0:6]
first_prev = lastMonth.replace(day=1)
lastMonth_prev = first_prev - timedelta(days=1)
day2 = lastMonth_prev.strftime("%Y%m%d")
#last day of 2 Months ago
sec_prev = lastMonth_prev.replace(day=1)
last2Month_prev = sec_prev - timedelta(days=1)
first_3month=last2Month_prev.replace(day=1)
day3 = last2Month_prev.strftime("%Y%m%d")
#last day of the 3 Months ago
firstday3=first_3month.strftime("%Y%m%d")
#first day of the 3 Months ago

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=15),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='mnp_incentive_monthly_report',
          default_args=args,
#          schedule_interval='0 6 6 * *'
          schedule_interval=None

)

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def end_dag():
    print('mnp incentive monthly report refreshed succesfully.')

with dag:
    mnp_incentive_monthly = BashOperator(task_id='mnp_incentive_monthly',bash_command='bash /nas/share05/dataOps_dev/mnp_incentive/DataOps_MNP_INCENTIVE.sh {0} {1} {2} {3} {4} {5}'.format(first_day,day1,day2,day3,firstday3,mnth_key),run_as_user = 'daasuser')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    mnp_incentive_monthly >> end_dag

