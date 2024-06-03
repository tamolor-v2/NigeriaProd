from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, EmailOperator, PythonOperator
import json, sys, os

commonPath = Variable.get("Ext_CommonPath", deserialize_json=True)
psqlURL = Variable.get("Ext_psqlURL", deserialize_json=True) 
emails = Variable.get("Ext_email", deserialize_json=True)
commonPathFeeds = Variable.get("Ext_CommonPathFeeds", deserialize_json=True)
commonSourcePath = Variable.get("Ext_CommonSourcePath", deserialize_json=True)

data = json.loads(open("{0}/config//EBU_GROUP_ESCALATIONS_config.json".format(commonPath)).read())
numrun = 0
numhdfs = 0
for x in data['steps']:
    if (x['stepname'] == "extract"):
        numrun = x['parameters']['dayRun']
        numhdfs = x['parameters']['runHdfs']
        
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

dag = DAG('Ext_EBU_GROUP_ESCALATIONS',
    default_args=args,
   ## schedule_interval= " 30 3 * * * ",
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
    provide_context=True,
    python_callable=pass_Date,
    xcom_push=True,
    dag=dag,
)

CreateDirectory = BashOperator(
    task_id='CreateDirectory',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn create_directory -jp {3}/config//EBU_GROUP_ESCALATIONS_config.json -d {0} -hr {1} -dr {2} -cp {4}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

DeleteOldFiles = BashOperator(
    task_id='DeleteOldFiles',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn delete_old_file -jp {3}/config//EBU_GROUP_ESCALATIONS_config.json -d {0} -hr {1} -dr {2} -cp {4}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract = BashOperator(
    task_id='Extract',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//EBU_GROUP_ESCALATIONS_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

MoveFiles = BashOperator(
    task_id='MoveFiles',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn move -jp {3}/config//EBU_GROUP_ESCALATIONS_config.json -d {0} -hr {1} -dr {2} -cp {4} -cs {5}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,commonSourcePath),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

alert = EmailOperator(
    task_id='alert',
    to='{0}'.format(emails),
    subject='Airflow EBU_GROUP_ESCALATIONS',
    html_content='Finsh extraction EBU_GROUP_ESCALATIONS <br>',
    dag=dag,
    ) 

takeDate >> DeleteOldFiles >> CreateDirectory >> Extract >> MoveFiles >> alert
