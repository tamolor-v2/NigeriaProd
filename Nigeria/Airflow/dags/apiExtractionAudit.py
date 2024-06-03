import airflow
import os, psycopg2, csv
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import PythonOperator, BashOperator

daydate = (datetime.today() - timedelta(hours=1)).strftime('%Y%m%d')

default_args = {
    'owner': 'API',
    'depends_on_past': False,
    'start_date': datetime(2021,3,17),
    'email': ['support@ligadata.com'],
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

def check_status(**kwargs):
    sql = ''
    path = kwargs['path']
    listOfFiles = os.listdir(path)
    conn = psycopg2.connect(database="audit", user = "paudit_user", password = "p@udit",host='10.1.197.142', port="5432")
    cursor = conn.cursor()
    print("Audit_log path: {}".format(path))
    try:
        for log_file in listOfFiles:
            print(log_file)
            insertSql = "insert into apiaudit.api_ext_audit (feed,datadate,runtype,extractionrunid,extractionstarttime,extractionelapsedtime,extractionrecordsread,extractionrecordswritten,extractionextimatedfilescount,extractionfilesindircount,extstatus,extremarks,conversionrunid,conversionstarttime,conversionelapsedtime,conversionrecordsread,conversionrecordswritten,conversionestimatedfiles,conversionfilesindircount,convstatus,convremarks,file_name) values"
            with open('{}{}'.format(path,log_file), "r") as f:
                reader = csv.reader(f, delimiter="|")
                for i, line in enumerate(reader):
                    if(len(line) < 20):
                        print("Invalid line: " + str(i) + " , " + str(line))
                    else:
                        if ( i != 0 ) :
                            print('feed[{}] = Extraction: {}, Conversion: {} {}'.format(line[0], line[10],line[19],line[20]))
                            sql += """,('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}','{21}')""".format(line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13],line[14],line[15],line[16],line[17],line[18],line[19],line[20],log_file)
            if (len(sql) > 0):
                cursor.execute("{} {} on conflict do nothing;".format(insertSql,sql).replace(" values ,"," values "))
            conn.commit()
            sql = ''
    except Exception as e:
        print('exception: ' + e)
    finally:
        cursor.close()
        conn.close()



dag = DAG('API_Extraction_Audit_Log',
          default_args=default_args,
          schedule_interval=' 0 * * * * ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )


success = BashOperator(
    task_id='success',
    bash_command='echo success ',
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
    )


Pushing_Audits_To_Postgres = PythonOperator(
    task_id='Pushing_Audits_To_Postgres',
    python_callable=check_status,
    op_kwargs={'path':'/nas/share30/apidfs/production/apidata/Audit/{}/'.format(daydate)},
    provide_context=True,
    xcom_push=True,
    run_as_user='daasuser',
    dag=dag
    )

Pushing_Audits_To_Postgres >> success
