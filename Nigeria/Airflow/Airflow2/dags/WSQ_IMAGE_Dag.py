from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
import json, sys, os
        
commonPath = Variable.get("Ext_CommonPath", deserialize_json=True)
psqlURL = Variable.get("Ext_psqlURL", deserialize_json=True) 
emails = Variable.get("Ext_email", deserialize_json=True)
commonPathFeeds = Variable.get("Ext_CommonPathFeeds", deserialize_json=True)
commonSourcePath = Variable.get("Ext_CommonSourcePath", deserialize_json=True)

data = json.loads(open("{0}/config//WSQ_IMAGE_config.json".format(commonPath)).read())
numrun = 0
numhdfs = 0
for x in data['steps']:
    if (x['stepname'] == "extract"):
        numrun = x['parameters']['dayRun']
        numhdfs = x['parameters']['runHdfs']

dateHDFS = datetime.today() + timedelta(days=int(numhdfs))
dateHDFStr = dateHDFS.strftime('%Y%m%d')
dateRun = datetime.today() + timedelta(days=int(numrun))
dateRunStr = dateRun.strftime('%Y%m%d')
today =  datetime.today()
todayStr = today.strftime('%Y%m%d')
dayTodayStr = today.strftime('%d')
runHourStr = today.strftime('%H')

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,3,20),
    'email': [emails],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('Ext_WSQ_IMAGE',
    default_args=args,
    schedule_interval= " 40 9 * * * ",
	catchup=False,
    max_active_runs=1
          )
		  
def pass_Date(ds, **kwargs):
    print(kwargs['dag_run'])
    date = None
    try:
        date = kwargs['dag_run'].conf['date']
    except:
        print("date is : " + str(date))
    if (date):
         dateRunStr = date
    else:
         dateRunStr = dateRun.strftime('%Y%m%d')
    kwargs['ti'].xcom_push(key='date', value=dateRunStr)
    print(dateRunStr)

takeDate = PythonOperator(
    task_id='takeDate',
    #provide_context=True,
    python_callable=pass_Date,
    #xcom_push=True,
    dag=dag,
)

CreateDirectory = BashOperator(
    task_id='CreateDirectory',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn create_directory -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

DeleteOldFiles = BashOperator(
    task_id='DeleteOldFiles',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn delete_old_file -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_0 = BashOperator(
    task_id='Extract_0',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 0 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_1 = BashOperator(
    task_id='Extract_1',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 1 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_2 = BashOperator(
    task_id='Extract_2',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 2 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_3 = BashOperator(
    task_id='Extract_3',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 3 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_4 = BashOperator(
    task_id='Extract_4',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 4 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_5 = BashOperator(
    task_id='Extract_5',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 5 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_6 = BashOperator(
    task_id='Extract_6',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 6 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_7 = BashOperator(
    task_id='Extract_7',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 7 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_8 = BashOperator(
    task_id='Extract_8',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 8 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

Extract_9 = BashOperator(
    task_id='Extract_9',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//WSQ_IMAGE_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 9 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    #provide_context=True,
    run_as_user='daasuser'
) 

alert = EmailOperator(
    task_id='alert',
    to='{0}'.format(emails),
    subject='Airflow WSQ_IMAGE',
    html_content='Finsh extraction WSQ_IMAGE <br>',
    dag=dag,
    ) 

takeDate >> DeleteOldFiles >> CreateDirectory  >> [Extract_0,Extract_1,Extract_2,Extract_3,Extract_4,Extract_5,Extract_6,Extract_7,Extract_8,Extract_9]   >> alert
