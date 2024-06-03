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

data = json.loads(open("{0}/config//NEWREG_BIOUPDT_POOL_config.json".format(commonPath)).read())
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

dag = DAG('Ext_NEWREG_BIOUPDT_POOL',
    default_args=args,
    schedule_interval= " 0 6 * * 1 ",
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
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn create_directory -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

DeleteOldFiles = BashOperator(
    task_id='DeleteOldFiles',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn delete_old_file -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_0 = BashOperator(
    task_id='Extract_0',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 00 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_1 = BashOperator(
    task_id='Extract_1',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 01 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_2 = BashOperator(
    task_id='Extract_2',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 02 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_3 = BashOperator(
    task_id='Extract_3',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 03 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_4 = BashOperator(
    task_id='Extract_4',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 04 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_5 = BashOperator(
    task_id='Extract_5',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 05 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_6 = BashOperator(
    task_id='Extract_6',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 06 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_7 = BashOperator(
    task_id='Extract_7',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 07 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_8 = BashOperator(
    task_id='Extract_8',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 08 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_9 = BashOperator(
    task_id='Extract_9',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 09 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_10 = BashOperator(
    task_id='Extract_10',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 10 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_11 = BashOperator(
    task_id='Extract_11',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 11 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_12 = BashOperator(
    task_id='Extract_12',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 12 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_13 = BashOperator(
    task_id='Extract_13',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 13 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_14 = BashOperator(
    task_id='Extract_14',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 14 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_15 = BashOperator(
    task_id='Extract_15',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 15 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_16 = BashOperator(
    task_id='Extract_16',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 16 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_17 = BashOperator(
    task_id='Extract_17',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 17 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_18 = BashOperator(
    task_id='Extract_18',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 18 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_19 = BashOperator(
    task_id='Extract_19',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 19 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_20 = BashOperator(
    task_id='Extract_20',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 20 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_21 = BashOperator(
    task_id='Extract_21',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 21 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_22 = BashOperator(
    task_id='Extract_22',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 22 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_23 = BashOperator(
    task_id='Extract_23',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 23 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_24 = BashOperator(
    task_id='Extract_24',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 24 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_25 = BashOperator(
    task_id='Extract_25',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 25 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_26 = BashOperator(
    task_id='Extract_26',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 26 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_27 = BashOperator(
    task_id='Extract_27',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 27 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_28 = BashOperator(
    task_id='Extract_28',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 28 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_29 = BashOperator(
    task_id='Extract_29',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 29 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_30 = BashOperator(
    task_id='Extract_30',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 30 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_31 = BashOperator(
    task_id='Extract_31',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 31 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_32 = BashOperator(
    task_id='Extract_32',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 32 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_33 = BashOperator(
    task_id='Extract_33',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 33 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_34 = BashOperator(
    task_id='Extract_34',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 34 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_35 = BashOperator(
    task_id='Extract_35',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 35 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_36 = BashOperator(
    task_id='Extract_36',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 36 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_37 = BashOperator(
    task_id='Extract_37',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 37 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_38 = BashOperator(
    task_id='Extract_38',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 38 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_39 = BashOperator(
    task_id='Extract_39',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 39 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_40 = BashOperator(
    task_id='Extract_40',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 40 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_41 = BashOperator(
    task_id='Extract_41',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 41 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_42 = BashOperator(
    task_id='Extract_42',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 42 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_43 = BashOperator(
    task_id='Extract_43',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 43 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_44 = BashOperator(
    task_id='Extract_44',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 44 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_45 = BashOperator(
    task_id='Extract_45',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 45 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_46 = BashOperator(
    task_id='Extract_46',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 46 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_47 = BashOperator(
    task_id='Extract_47',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 47 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_48 = BashOperator(
    task_id='Extract_48',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 48 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_49 = BashOperator(
    task_id='Extract_49',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 49 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_50 = BashOperator(
    task_id='Extract_50',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 50 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_51 = BashOperator(
    task_id='Extract_51',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 51 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_52 = BashOperator(
    task_id='Extract_52',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 52 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_53 = BashOperator(
    task_id='Extract_53',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 53 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_54 = BashOperator(
    task_id='Extract_54',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 54 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_55 = BashOperator(
    task_id='Extract_55',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 55 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_56 = BashOperator(
    task_id='Extract_56',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 56 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_57 = BashOperator(
    task_id='Extract_57',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 57 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_58 = BashOperator(
    task_id='Extract_58',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 58 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_59 = BashOperator(
    task_id='Extract_59',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 59 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_60 = BashOperator(
    task_id='Extract_60',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 60 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_61 = BashOperator(
    task_id='Extract_61',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 61 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_62 = BashOperator(
    task_id='Extract_62',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 62 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_63 = BashOperator(
    task_id='Extract_63',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 63 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_64 = BashOperator(
    task_id='Extract_64',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 64 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_65 = BashOperator(
    task_id='Extract_65',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 65 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_66 = BashOperator(
    task_id='Extract_66',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 66 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_67 = BashOperator(
    task_id='Extract_67',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 67 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_68 = BashOperator(
    task_id='Extract_68',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 68 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_69 = BashOperator(
    task_id='Extract_69',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 69 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_70 = BashOperator(
    task_id='Extract_70',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 70 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_71 = BashOperator(
    task_id='Extract_71',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 71 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_72 = BashOperator(
    task_id='Extract_72',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 72 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_73 = BashOperator(
    task_id='Extract_73',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 73 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_74 = BashOperator(
    task_id='Extract_74',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 74 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_75 = BashOperator(
    task_id='Extract_75',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 75 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_76 = BashOperator(
    task_id='Extract_76',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 76 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_77 = BashOperator(
    task_id='Extract_77',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 77 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_78 = BashOperator(
    task_id='Extract_78',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 78 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_79 = BashOperator(
    task_id='Extract_79',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 79 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_80 = BashOperator(
    task_id='Extract_80',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 80 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_81 = BashOperator(
    task_id='Extract_81',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 81 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_82 = BashOperator(
    task_id='Extract_82',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 82 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_83 = BashOperator(
    task_id='Extract_83',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 83 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_84 = BashOperator(
    task_id='Extract_84',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 84 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_85 = BashOperator(
    task_id='Extract_85',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 85 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_86 = BashOperator(
    task_id='Extract_86',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 86 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_87 = BashOperator(
    task_id='Extract_87',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 87 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_88 = BashOperator(
    task_id='Extract_88',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 88 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_89 = BashOperator(
    task_id='Extract_89',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 89 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_90 = BashOperator(
    task_id='Extract_90',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 90 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_91 = BashOperator(
    task_id='Extract_91',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 91 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_92 = BashOperator(
    task_id='Extract_92',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 92 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_93 = BashOperator(
    task_id='Extract_93',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 93 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_94 = BashOperator(
    task_id='Extract_94',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 94 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_95 = BashOperator(
    task_id='Extract_95',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 95 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_96 = BashOperator(
    task_id='Extract_96',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 96 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_97 = BashOperator(
    task_id='Extract_97',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 97 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_98 = BashOperator(
    task_id='Extract_98',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 98 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

Extract_99 = BashOperator(
    task_id='Extract_99',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn extract -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} {5} -ms 99 '.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,psqlURL),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

MoveFiles = BashOperator(
    task_id='MoveFiles',
    bash_command='java -Dlog4j.configurationFile={3}/bin/log4j.xml -cp {3}/lib/ojdbc6.jar:{3}/lib/ExtractionJobsBuilder-1.0.jar  com.ligadata.extractjb.Main -sn move -jp {3}/config//NEWREG_BIOUPDT_POOL_config.json -d {0} -hr {1} -dr {2} -cp {4} -cs {5}'.format('{{ ti.xcom_pull(key="date" , task_ids="takeDate") }}',runHourStr,dayTodayStr,commonPath,commonPathFeeds,commonSourcePath),
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
) 

alert = EmailOperator(
    task_id='alert',
    to='{0}'.format(emails),
    subject='Airflow NEWREG_BIOUPDT_POOL',
    html_content='Finsh extraction NEWREG_BIOUPDT_POOL <br>',
    dag=dag,
    )

dump1 =  BashOperator(
    task_id='dump1',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump2 =  BashOperator(
    task_id='dump2',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump3 =  BashOperator(
    task_id='dump3',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump4 =  BashOperator(
    task_id='dump4',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump5 =  BashOperator(
    task_id='dump5',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump6 =  BashOperator(
    task_id='dump6',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump7 =  BashOperator(
    task_id='dump7',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
dump8 =  BashOperator(
    task_id='dump8',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)
 
dump9 =  BashOperator(
    task_id='dump9',
    bash_command="echo dump",
    dag=dag,
    provide_context=True,
    run_as_user='daasuser'
)

takeDate >> DeleteOldFiles >> CreateDirectory  >> [Extract_0,Extract_1,Extract_2,Extract_3,Extract_4,Extract_5,Extract_6,Extract_7,Extract_8,Extract_9]>> dump1 >> [Extract_10,Extract_11,Extract_12,Extract_13,Extract_14,Extract_15,Extract_16,Extract_17,Extract_18,Extract_19] >> dump2 >> [Extract_20,Extract_21,Extract_22,Extract_23,Extract_24,Extract_25,Extract_26,Extract_27,Extract_28,Extract_29] >> dump3 >> [Extract_30,Extract_31,Extract_32,Extract_33,Extract_34,Extract_35,Extract_36,Extract_37,Extract_38,Extract_39] >> dump4 >> [Extract_40,Extract_41,Extract_42,Extract_43,Extract_44,Extract_45,Extract_46,Extract_47,Extract_48,Extract_49] >> dump9 >> [Extract_50,Extract_51,Extract_52,Extract_53,Extract_54,Extract_55,Extract_56,Extract_57,Extract_58,Extract_59] >> dump5 >> [Extract_60,Extract_61,Extract_62,Extract_63,Extract_64,Extract_65,Extract_66,Extract_67,Extract_68,Extract_69] >> dump6 >> [Extract_70,Extract_71,Extract_72,Extract_73,Extract_74,Extract_75,Extract_76,Extract_77,Extract_78,Extract_79] >> dump7 >> [Extract_80,Extract_81,Extract_82,Extract_83,Extract_84,Extract_85,Extract_86,Extract_87,Extract_88,Extract_89] >> dump8 >> [Extract_90,Extract_91,Extract_92,Extract_93,Extract_94,Extract_95,Extract_96,Extract_97,Extract_98,Extract_99] >> MoveFiles >> alert
