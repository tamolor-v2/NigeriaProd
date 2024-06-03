import airflow
import csv
import json, sys, time, shutil, gzip, os, psycopg2, fnmatch
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
#from airflow.operators import PythonOperator, BashOperator, EmailOperator, BranchPythonOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

today = datetime.today()
strToday= today .strftime('%Y-%m-%d-%H')
#daydate= (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
daydate= today .strftime('%Y-%m-%d')
#daydate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
audit_date= (datetime.today() - timedelta(hours=1)).strftime('%Y-%m-%d-%H')
run_hour= today .strftime('%H')
if(run_hour == '00'):
    daydate = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    audit_date = (datetime.today() - timedelta(hours=1,days=1)).strftime('%Y-%m-%d-%H')

def rename_files_to_processing(**kwargs):
    path = kwargs['path']
    listOfFiles = os.listdir(path)
    pattern = kwargs['pattern']
    for entry in listOfFiles:
        if fnmatch.fnmatch(entry, pattern):
            new_file_name = '{}/processing_{}'.format(path,entry)
            os.rename('{}/{}'.format(path,entry),new_file_name)
            print('{}/{}'.format(path,entry),new_file_name)

def extract_audits(**kwargs):
    print("date to extraact data from: {}".format(daydate))
    path = kwargs['path']
    node_name = kwargs['node_name']
    print('running the following command: ssh -o StrictHostKeyChecking=no {0} ls  /opt/apache-tomcat-8.5.31/logs/Archive_DaasAPI_Audit/{1}/ |grep -v done'.format(node_name,daydate))
    listOfFiles = os.popen('ssh -o StrictHostKeyChecking=no {0} ls  /opt/apache-tomcat-8.5.31/logs/Archive_DaasAPI_Audit/{1}/ |grep -v done'.format(node_name,daydate)).read().split('\n')
    print(listOfFiles)
    if (listOfFiles):
        del listOfFiles[len(listOfFiles)-1]
        for log_file in listOfFiles:
            print(log_file)
            new_file_name = log_file.replace('gz','text')
            os.popen('ssh -o StrictHostKeyChecking=no {2} python /nas/share05/tools/API_logs/apilogs_with_rename_new.py -in /opt/apache-tomcat-8.5.31/logs/Archive_DaasAPI_Audit/{0}/{3} -out /nas/share05/tools/API_logs/logs/{4} -nc -nid {2}'.format(daydate,audit_date,node_name,log_file,new_file_name))


def rename_files_to_processed(**kwargs):
    path = kwargs['path']
    listOfFiles = os.listdir(path)
    # pattern = "processing*"
    pattern = kwargs['pattern']
    for entry in listOfFiles:
        if fnmatch.fnmatch(entry, pattern):
            new_file_name = '{}/{}'.format(path,entry.replace("processing","processed"))
            os.rename('{}/{}'.format(path,entry),new_file_name)
            print('{}/{}'.format(path,entry),new_file_name)

def insert_users_audit(**kwargs):
    try:
        path = kwargs['full_audit_Path']
        conn = psycopg2.connect(database="audit", user = "paudit_user", password = "p@udit",host='10.1.197.142', port="5432")
        cursor = conn.cursor()
        sql = ''
        pattern = "processing*"
        files = []
        listOfFiles = os.listdir(path)
        for entry in listOfFiles:
            if fnmatch.fnmatch(entry, pattern):
                files.append(entry)
        for log_file in files:
            print(log_file)
            with open('{}/{}'.format(path,log_file), "r") as f:
                reader = csv.reader(f, delimiter="|")
                sql_insert = "insert into apiaudit.apisvc (node_name,remote_add,startepoch,reqdttm,msg,usr,  app,reqkey,startdt,enddt,duration,recs,status) VALUES"
                for i, line in enumerate(reader):
                    sql += """,('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}')""".format(line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12])
                    if (i % 10000 == 0 and i != 0):
                        print("**********************************************{}".format(i))
                        cursor.execute("{} {}".format(sql_insert,sql).replace(" VALUES ,"," VALUES "))
                        sql = ''
            if (sql != ''):
                cursor.execute("{} {}".format(sql_insert,sql).replace(" VALUES ,"," VALUES "))
            conn.commit()
            print('commiting')
        print('closing connection and cursor')
        cursor.close()
        conn.close()
    except Exception as e:
        print('closing connection and cursor after exception')
#        conn.commit()
        cursor.close()
        conn.close()
        print(e)
        sys.exit(1)
#    conn.commit()
#    print('commiting')

default_args = {
    'owner': 'API',
    'depends_on_past': False,
    'start_date': datetime(2020,4,4),
    'email': ['support@ligadata.com'],
    'email_on_failure': ['support@ligadata.com'],
    'email_on_retry': True,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG('API_User_Audits_To_Psql',
          default_args=default_args,
          schedule_interval=' 10 * * * * ',
          catchup=False,
          concurrency=2,
          max_active_runs=1
          )


Rename_users_audits_to_processing = PythonOperator(
    task_id='Rename_users_audits_to_processing',
    python_callable=rename_files_to_processing,
    run_as_user='daasuser',
    op_kwargs={'path':'/nas/share05/tools/API_logs/logs/', 'pattern':'DaasAPI_Audit_*'},
    provide_context=True,
    dag=dag
    )


Rename_users_audits_to_processed = PythonOperator(
    task_id='Rename_users_audits_to_processed',
    python_callable=rename_files_to_processed,
    run_as_user='daasuser',
    op_kwargs={'path':'/nas/share05/tools/API_logs/logs/', 'pattern':'processing*'},
    provide_context=True,
    dag=dag
    )


Pushing_users_audits_into_postgress = PythonOperator(
    task_id='Pushing_users_audits_into_postgress',
    python_callable=insert_users_audit,
    run_as_user='daasuser',
    op_kwargs={'full_audit_Path':'/nas/share05/tools/API_logs/logs/'},
    provide_context=True,
    dag=dag
    )


extract_audits_from_datanode01090 = PythonOperator(
    task_id='extract_audits_from_datanode01090',
    python_callable=extract_audits,
    run_as_user='daasuser',
    op_kwargs={'path':'/opt/apache-tomcat-8.5.31/logs/Archive_DaasAPI_Audit/{0}/'.format(daydate), 'node_name':'datanode01090'},
    provide_context=True,
    dag=dag
    )

extract_audits_from_datanode01093 = PythonOperator(
    task_id='extract_audits_from_datanode01093',
    python_callable=extract_audits,
    run_as_user='daasuser',
    op_kwargs={'path':'/opt/apache-tomcat-8.5.31/logs/Archive_DaasAPI_Audit/{0}/'.format(daydate),'node_name':'datanode01093'},
    provide_context=True,
    dag=dag
    )

extract_audits_from_datanode01040 = PythonOperator(
    task_id='extract_audits_from_datanode01040',
    python_callable=extract_audits,
    run_as_user='daasuser',
    op_kwargs={'path':'/opt/apache-tomcat-8.5.31/logs/Archive_DaasAPI/{0}/'.format(daydate),'node_name':'datanode01040'},
    provide_context=True,
    dag=dag
    )

success = BashOperator(
    task_id='success',
    bash_command='echo success ',
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
    )

extract_audits_from_datanode01090 >> Rename_users_audits_to_processing >> Pushing_users_audits_into_postgress >> Rename_users_audits_to_processed >> success
extract_audits_from_datanode01093 >> Rename_users_audits_to_processing >> Pushing_users_audits_into_postgress >> Rename_users_audits_to_processed >> success
extract_audits_from_datanode01040 >> Rename_users_audits_to_processing >> Pushing_users_audits_into_postgress >> Rename_users_audits_to_processed >> success

