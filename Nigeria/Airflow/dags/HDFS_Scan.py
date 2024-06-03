from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, EmailOperator, PythonOperator
import json, sys, os

emails = 'support@ligadata.com'

HDFS_report_path = Variable.get("HDFS_Scan_report_path", deserialize_json=True)
local_path = Variable.get("HDFS_Scan_local_report_path", deserialize_json=True)
schema = Variable.get("HDFS_Scan_schema", deserialize_json=True)
table = Variable.get("HDFS_Scan_table", deserialize_json=True)



today =  datetime.today()
todayStr = today.strftime('%Y%m%d')


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,3,20),
    'email': emails,
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('HDFS_Scan',
    default_args=args,
    schedule_interval= " 0 13 * * * ",
        catchup=False,
    max_active_runs=1
          )

def create_partition(**kwargs):
    check = os.system("hadoop fs -test -e %s"%kwargs['path'])
    if check is not 0:
            print("running the following command: %s"%"hadoop fs -mkdir -p %s"%kwargs['path'])
            check = os.system("hadoop fs -mkdir -p %s"%kwargs['path'])
            print("result of cmd: %s"%type(check))
            if check is not 0:
                exit(1)
    else:
        print("partition is already exist")



HDFS_Scan = BashOperator(
    task_id='HDFS_Scan',
    bash_command="bash /nas/share05/tools/hdfs_report/hdfs_new.sh {0} {1} {2} {3} {4}".format(todayStr, schema, table, HDFS_report_path, local_path),
    dag=dag,
    run_as_user='daasuser'
)


Send_Email = BashOperator(
    task_id='Send_Email',
    bash_command="python /nas/share05/tools/hdfs_report/email/Send_email.py /nas/share05/tools/hdfs_report/email/conf.json {0}".format(todayStr),
    dag=dag,
    run_as_user='daasuser'
)


alert = EmailOperator(
    task_id='alert',
    to='{0}'.format(emails),
    subject='Airflow HDFS_SCAN',
    html_content='HDFS Scan job has been finished',
    dag=dag,
    )

HDFS_Scan >> Send_Email >> alert
