import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
day = yesterday.strftime('%Y%m%d')
yesterdayStr = today.strftime('%Y%m%d')
sleeptime= 2*1000* 60

default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date': datetime(2020,2,23),
    'email': ['m.abdin@ligadata.com'],
    'email_on_failure': True,
    'email_on_success':True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('Hello_World', default_args=default_args, schedule_interval=None)


#Hello_Word_Task = BashOperator(
#    task_id='Hello_World_Task',
#    bash_command= 'dcho Hello_World',
#    email_on_failure=True,
#    email_on_success=True,
#    email=['a.monem@ligadata.com'],
#    subject='Airflow Alert',
#    html_content=""" <h3>Email Test</h3> """,
#    dag=dag,
#    run_as_user='daasuser',
#)

Hello_Word_Task = BashOperator(
    task_id='Hello_World_Task',
    bash_command= 'date ',
    dag=dag,
    run_as_user='daasuser',
)

def returnDag2(parant,args,schedule):
    dag = DAG('{0}.{1}'.format(parant,'Hello_World'), default_args=args, schedule_interval=schedule)
    return dag
