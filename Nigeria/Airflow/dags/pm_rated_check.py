from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, BashOperator, EmailOperator, BranchPythonOperator
import json, sys, time, shutil, gzip, os, psycopg2
from airflow.sensors.external_task_sensor import ExternalTaskSensor

pathConfile = Variable.get("pathConfigJSONFile", deserialize_json=True)
data = json.loads(open(pathConfile).read())

for k,v in data.items():
    for key,vars in v.items():
        exec("%s='%s'" % (key,vars))
        
today = datetime.today()
run_hour = today.strftime('%H')#runHour
date_str = today.strftime('%Y%m%d')
run_date_2 = datetime.today() + timedelta(hours=int(toHour))
run_date_1 = datetime.today() + timedelta(hours=int(fromHour))
view_date = datetime.today() + timedelta(hours=int(viewHour) )
view_date_str = view_date.strftime('%Y%m%d')
run_1 = run_date_1.strftime('%Y%m%d')
run_2 = run_date_2.strftime('%Y%m%d')

rerunhours = ['0003','0304','0405','0506','0607','0708','0809','0910','1011','1112','1213','1314','1415','1516','1617','1718','1819','1920','2021','2122','2223']
def fill_hours(run_hour):
    if ( 8 <= int(run_hour) <= 11):
       return  'check_hour0003','check_hour0304','check_hour0405','check_hour0506'
    elif ( 12 <= int(run_hour) <= 15):
        return 'check_hour0607','check_hour0708','check_hour0809','check_hour0910'
    elif ( 16 <= int(run_hour) <= 19 ):
        return 'check_hour1011','check_hour1112','check_hour1213','check_hour1314'
    elif ( 20 <= int(run_hour) <= 23):
        return 'check_hour1415','check_hour1516','check_hour1617','check_hour1718'
    elif ( 1 <= int(run_hour) <= 5):
        return 'check_hour0003','check_hour0304','check_hour0405','check_hour0506','check_hour0607','check_hour0708','check_hour0809','check_hour0910','check_hour1011','check_hour1112','check_hour1213','check_hour1314','check_hour1415','check_hour1516','check_hour1617','check_hour1718','check_hour1819','check_hour1920','check_hour2021','check_hour2122','check_hour2223'


connectionSQL = "'(DESCRIPTION=(ADDRESS=(PROTOCOL={2})(HOST={3})(PORT={4}))(CONNECT_DATA=(SERVER={5})(SERVICE_NAME={6})))'".format(SQLPlus_userNameCheck,SQLPlus_passwordCheck,SQLPlus_PROTOCOLCheck,SQLPlus_HOSTCheck,SQLPlus_PORTCheck,SQLPlus_SERVERCheck,SQLPlus_SERVICE_NAMECheck)


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,3,20),
    'email': ['m.abdin@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('PM_Rated_check',
    default_args=args,
    #schedule_interval= '0 7,11,15,19,0 * * *',
    schedule_interval= None,
    catchup=False,
    concurrency=3,
    max_active_runs=1
          )

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def getDataMySql(date_str,hour):
    try:
        conn = psycopg2.connect(database=databasesql, user = usersql, password = passwordsql,host=hostsql, port=portsql)
        cursor = conn.cursor()
        sql = """select run_hour,number_of_record_file from extractionjobs.auditextract where extract_date = '{0}' and run_hour = '{1}' and feed_id = 160""".format(date_str,hour)
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        if len(result) == 0:
    	    x = 0
        else:
            x = result[0][1]
        print(x)
        cursor.close()
        conn.close()
        return x
    except Exception as e:
        print(e)
        cursor.close()
        conn.close()

def getDataSqlPlus(runFromHour,runToHour,view_date_str):
    command = os.popen("bash {0} {1} {1}{2}000 {1}{3}000".format(SQLCheckFilePath,view_date_str,runFromHour,runToHour)).readlines()
    if RepresentsInt(command[3]):
        return int(command[3])
    else:
        for line in command:
            print(line)
            sys.exit(1)

def checks(**kwargs):
    runFromHour= kwargs['runFromHour']
    runToHour= kwargs['runToHour']
    view_date_str= kwargs['view_date_str']
    hour = runFromHour+runToHour
    full_sourcePath= kwargs['full_sourcePath']
    sourcePath= kwargs['sourcePath']
    tempPath= kwargs['tempPath']
    SQLCheckFilePath = kwargs['SQLCheckFilePath']
    if os.path.exists(tempPath):
        if os.path.exists(sourcePath):
            if not os.path.isdir(full_sourcePath): 
                try:
                    os.mkdir(full_sourcePath)
                except Exception as e:
                    print(e)
                    sys.exit(1)
        else:
            print("the source path dose not exist")
            sys.exit(1)
    else:
        print("the temp path dose not exist")
        sys.exit(1)
    numoflinetemp = getDataMySql(view_date_str,hour)
    i = 0
    while(i<=4):
        if(numoflinetemp == 0):
            print("sleep 15 min")
            time.sleep(900)
            numoflinetemp = getDataMySql(view_date_str,hour)
            i = i+1
        else:
            break
    numoflinesource = getDataSqlPlus(runFromHour,runToHour,view_date_str)
    print("#tmp_{0}".format(numoflinetemp))
    print("#source_{0}".format(numoflinesource))
    if 0 <= numoflinesource-numoflinetemp < 50 :
        return 'Success'
    else:
        t = 'Extraction_PMRated_{0}{1}'.format(runFromHour,runToHour)
        return t

	
def gzipfile(**kwargs):
    full_path = kwargs['full_path']
    full_file_gzname = kwargs['full_file_gzname']
    full_path = full_path+'.lst'
    if os.path.exists(full_path):
        print(full_file_gzname)
        print(full_path)
        command = os.popen('gzip {0}'.format(full_path))
    else:
        print("the file {0} dose not exists".format(full_path))
        sys.exit(1)
    
    
def movefile(**kwargs):
    tempPath = kwargs['tempPath']
    full_file_gzname = kwargs['full_file_gzname']
    full_sourcePath = kwargs['full_sourcePath']
    try:
        path = os.path.join(tempPath,full_file_gzname)
        print("sleep 3 minutes")
        time.sleep(170)
        if os.path.exists(path):
            count = len(gzip.open(path).readlines())
            print("lineCount = {0}".format(count))
            size_file = os.path.getsize(path)
            print("sizeFile = {0}".format(size_file))
            if (size_file > 0 and count > 20 or count == 0):
                print(path)
                kwargs['ti'].xcom_push(key='countline', value=count)
                #os.mkdir(full_sourcePath)    
                print(full_sourcePath)
                shutil.move(path,full_sourcePath)
            else:
                print("the size file is 0 or error in file")
                os.remove(path)
                print("removed file {0}".format(path))
                sys.exit(1)
        else:
            print("the file {0} dose not exists".format(path))
            sys.exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)
    
def putdata(**kwargs):
    full_sourcePath= kwargs['full_sourcePath']
    full_file_gzname= kwargs['full_file_gzname']
    usersql= kwargs['usersql']
    passwordsql= kwargs['passwordsql']
    databasesql= kwargs['databasesql']
    hostsql= kwargs['hostsql']
    portsql = kwargs['portsql']
    view_date_str= kwargs['view_date_str']
    task = kwargs['task']
    run_hour2= kwargs['run_hour2']
    run_hour1= kwargs['run_hour1']
    try:
        count = kwargs['ti'].xcom_pull(key="countline" , task_ids=task)
        path = os.path.join(full_sourcePath,full_file_gzname)
        conn = psycopg2.connect(database=databasesql, user = usersql, password = passwordsql,host=hostsql, port=portsql)
        cursor = conn.cursor()
        sql = """select run_hour,number_of_record_file from extractionjobs.auditextract where extract_date = '{0}' and run_hour = '{1}' and feed_id = 160""".format(view_date_str,run_hour2,run_hour1)
        cursor.execute(sql)
        result = cursor.fetchall()
        numoflinesource = getDataSqlPlus(run_hour2,run_hour1,view_date_str)
        if len(result) == 0:
            status = ""
            if(count == numoflinesource):
                status = 'success'
            else:
                status = 'failed'
            sql = """insert into extractionjobs.auditextract (feed_id,extract_date,run_hour,number_of_record_file,number_of_file,number_of_record_db,status_extract) values (160,'{0}','{1}{2}',{3},1,{4},'{5}')""".format(view_date_str,run_hour2,run_hour1,count,numoflinesource,status)
        else:
            status = ""
            if(count == numoflinesource):
                status = 'success'
            else:
                status = 'failed'
            sql = """UPDATE extractionjobs.auditextract SET number_of_record_file='{3}',status_extract = '{4}'  WHERE feed_id = 160 AND extract_date='{0}' AND run_hour='{1}{2}'""".format(view_date_str,run_hour2,run_hour1,count,status)
        print(sql)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(e)
        sys.exit(1)

checkHour = BranchPythonOperator(
    task_id='checkHour',
    python_callable=fill_hours,
    run_as_user='daasuser',
    op_args=[run_hour],
    dag=dag
    )

Success = BashOperator(
        task_id='Success',
        bash_command="echo end",
        dag=dag,
        run_as_user='daasuser'
        )
 
for hour in rerunhours:

    full_file_name = "{0}{1}{2}{3}".format(view_date_str,file_name,hour[0:2],hour[2:4])
    full_path = os.path.join(tempPath,full_file_name)
    full_file_gzname = "{0}.lst.gz".format(full_file_name)
    full_sourcePath = os.path.join(sourcePath,view_date_str)
    
    check = BranchPythonOperator(
        task_id='check_hour{0}{1}'.format(hour[0:2],hour[2:4]),
        python_callable=checks,
        op_kwargs={'runFromHour':hour[0:2],'runToHour':hour[2:4],'view_date_str':view_date_str,'full_sourcePath':full_sourcePath,'sourcePath':sourcePath,'tempPath':tempPath,'SQLCheckFilePath':SQLCheckFilePath},
        run_as_user='daasuser',
        dag=dag
        )

    Extraction_PMRated = BashOperator(
        task_id='Extraction_PMRated_%s'%hour,
        bash_command="bash {9} {0} {1} {2} {3} {4} {5} {6} {7} {8}".format(hour[0:2],hour[2:4],view_date_str,view_date_str,view_date_str,full_path,SQLPlus_userName,SQLPlus_password,connectionSQL,SQLFilePath),
	dag=dag,
	run_as_user='daasuser'
	)
    
    gzip_file = PythonOperator(
        task_id='gzip_file_%s'%hour,
        python_callable=gzipfile,
        dag=dag,
        provide_context=True,
        xcom_push=True,
        op_kwargs={'full_path':full_path,'full_file_gzname':full_file_gzname},
        run_as_user='daasuser'
        )

    move_files = PythonOperator(
        task_id='move_files_%s'%hour,
        python_callable=movefile,
        dag=dag,
        provide_context=True,
        xcom_push=True,
        op_kwargs={'tempPath':tempPath,'full_file_gzname':full_file_gzname,'full_sourcePath':full_sourcePath},
        run_as_user='daasuser'
        )
    put_ToDatabase = PythonOperator(
        task_id='put_ToDatabase_%s'%hour,
        python_callable=putdata,
        dag=dag,
        provide_context=True,
        xcom_push=True,
        op_kwargs={'full_sourcePath':full_sourcePath,'full_file_gzname':full_file_gzname,'view_date_str':view_date_str,'run_hour2':hour[0:2],'run_hour1':hour[2:4],'usersql':usersql,'passwordsql':passwordsql,'databasesql':databasesql,'hostsql':hostsql,'portsql':portsql,'task':"move_files_%s"%hour},
	run_as_user='daasuser'
	    )
       
    checkHour >> check >> Extraction_PMRated >> gzip_file >> move_files >> put_ToDatabase >> Success
    checkHour >> check >> Success
