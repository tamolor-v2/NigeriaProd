from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
import json, sys, time, shutil, gzip, os, psycopg2

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

rerunhours = ['0003','0304','0405','0506','0607','0708','0809','0910','1011','1112','1213','1314','1415','1516','1617','1718','1819','1920','2021','2122','2223','2300','0001']
def fill_hours(run_hour,sourcePath,view_date_str):
    full_sourcepath = os.path.join(sourcePath,view_date_str)
    if not os.path.isdir(full_sourcePath):
        os.mkdir(full_sourcePath)

    if (int(run_hour) == 7):
       return  'checkTable'
    elif (int(run_hour) == 8):
        return 'Extraction_PMRated_0607'
    elif (int(run_hour) == 9):
        return 'Extraction_PMRated_0708'
    elif (int(run_hour) == 10):
        return 'Extraction_PMRated_0809'
    elif (int(run_hour) == 11):
        return 'Extraction_PMRated_0910'
    elif (int(run_hour) == 12):
        return 'Extraction_PMRated_1011'
    elif (int(run_hour) == 13):
        return 'Extraction_PMRated_1112'
    elif (int(run_hour) == 14):
        return 'Extraction_PMRated_1213'
    elif (int(run_hour) == 15):
        return 'Extraction_PMRated_1314' 
    elif (int(run_hour) == 16):
        return 'Extraction_PMRated_1415'
    elif (int(run_hour) == 17):
        return 'Extraction_PMRated_1516'
    elif (int(run_hour) == 18):
        return 'Extraction_PMRated_1617'
    elif (int(run_hour) == 19):
        return 'Extraction_PMRated_1718'
    elif (int(run_hour) == 20):
        return 'Extraction_PMRated_1819'
    elif (int(run_hour) == 21):
        return 'Extraction_PMRated_1920'
    elif (int(run_hour) == 22):
        return 'Extraction_PMRated_2021'
    elif (int(run_hour) == 23):
        return 'Extraction_PMRated_2122'
    elif (int(run_hour) == 00):
        return 'Extraction_PMRated_2223'
    elif (int(run_hour) == 1):
        return 'Extraction_PMRated_2300'
    elif (int(run_hour) == 2):
        return 'Extraction_PMRated_0001'


connectionSQL = "'(DESCRIPTION=(ADDRESS=(PROTOCOL={2})(HOST={3})(PORT={4}))(CONNECT_DATA=(SERVER={5})(SERVICE_NAME={6})))'".format(SQLPlus_userName,SQLPlus_password,SQLPlus_PROTOCOL,SQLPlus_HOST,SQLPlus_PORT,SQLPlus_SERVER,SQLPlus_SERVICE_NAME)


args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date': datetime(2020,3,20),
    'email': ['m.abdin@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('PM_Rated',
    default_args=args,
    schedule_interval= ' 00 6-23,00-1 * * * ',
    catchup=False,
    concurrency=5,
    max_active_runs=2
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
    print("{0} {3}{1}000 {4}{2}000".format(view_date_str,runFromHour,runToHour,run_2,run_1))
    command = os.popen("bash {0} {1} {4}{2}000 {5}{3}000".format(SQLCheckFilePath,view_date_str,runFromHour,runToHour,run_2,run_1)).readlines()
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
    numoflinesource = getDataSqlPlus(runFromHour,runToHour,view_date_str)
    print("#tmp_{0}".format(numoflinetemp))
    print("#source_{0}".format(numoflinesource))
    if 0 <= numoflinesource-numoflinetemp < 50 :
        return 'alert'
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
    hour = run_hour2+run_hour1
    numoflinetemp = getDataSqlPlus(run_hour2,run_hour1,view_date_str)
    try:
        status = ""
        count = kwargs['ti'].xcom_pull(key="countline" , task_ids=task)
        kwargs['ti'].xcom_push(key='countline', value=count)
        path = os.path.join(full_sourcePath,full_file_gzname)
        if(count == numoflinetemp):
            status = 'success'
        else:
            status = 'failed'
        conn = psycopg2.connect(database=databasesql, user = usersql, password = passwordsql,host=hostsql, port=portsql)
        cursor = conn.cursor()
        sql = """insert into extractionjobs.auditextract (feed_id,extract_date,run_hour,number_of_record_file,number_of_file,number_of_record_db,status_extract) values (160,'{0}','{1}{2}',{3},1,{4},'{5}')""".format(view_date_str,run_hour2,run_hour1,count,numoflinetemp,status)
        print(sql)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)
        sys.exit(1)
        
def checkTable(**kwargs):
    sourcePath= kwargs['sourcePath']
    SQLCheckFilePath = kwargs['SQLCheckFilePath']
    full_sourcepath = os.path.join(sourcePath,view_date_str)
    try:
        if not os.path.isdir(full_sourcePath):
            os.mkdir(full_sourcePath)
            print("create directory :{0}".format(full_sourcepath))
        command = os.popen("bash {0} {1} {1}00000 {1}01000".format(SQLCheckFilePath,run_1)).readlines()
        if RepresentsInt(command[3]):
            print("table exists")
        else:
            for line in command:
                print(line)
            sys.exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)
        
Dedup = BashOperator(
    task_id='Dedup',
    bash_command="bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new.sh -f WBS_PM_RATED_CDRS -p m1004 -d {0} -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_PMRated_$(date +%Y%m%d%H%M%S).txt".format(view_date_str),
    dag=dag,
    trigger_rule='none_failed',
    run_as_user='daasuser'
     )

checkHour = BranchPythonOperator(
    task_id='checkHour',
    python_callable=fill_hours,
    run_as_user='daasuser',
    op_args=[run_hour,sourcePath,view_date_str],
    dag=dag
    )
checkTable = PythonOperator(
    task_id='checkTable',
    python_callable=checkTable,
    dag=dag,
    
    
    op_kwargs={'sourcePath':sourcePath,'SQLCheckFilePath':SQLCheckFilePath},
    run_as_user='daasuser'
)
 
for hour in rerunhours:

    full_file_name = "{0}{1}{2}{3}".format(view_date_str,file_name,hour[0:2],hour[2:4])
    full_path = os.path.join(tempPath,full_file_name)
    full_file_gzname = "{0}.lst.gz".format(full_file_name)
    full_sourcePath = os.path.join(sourcePath,view_date_str)

    Extraction_PMRated = BashOperator(
    task_id='Extraction_PMRated_%s'%hour,
    bash_command="bash {9} {0} {1} {2} {3} {4} {5} {6} {7} {8}".format(hour[0:2],hour[2:4],run_2,run_1,view_date_str,full_path,SQLPlus_userName,SQLPlus_password,connectionSQL,SQLFilePath),
    dag=dag,
    run_as_user='daasuser'
    )
    gzip_file = PythonOperator(
        task_id='gzip_file_%s'%hour,
        python_callable=gzipfile,
        dag=dag,
        
        
        op_kwargs={'full_path':full_path,'full_file_gzname':full_file_gzname},
        run_as_user='daasuser'
        )
    move_files = PythonOperator(
        task_id='move_files_%s'%hour,
        python_callable=movefile,
        dag=dag,
        
        
        op_kwargs={'tempPath':tempPath,'full_file_gzname':full_file_gzname,'full_sourcePath':full_sourcePath},
        run_as_user='daasuser'
        )
    put_ToDatabase = PythonOperator(
        task_id='put_ToDatabase_%s'%hour,
        python_callable=putdata,
        dag=dag,
        
        
        op_kwargs={'full_sourcePath':full_sourcePath,'full_file_gzname':full_file_gzname,'view_date_str':view_date_str,'run_hour2':hour[0:2],'run_hour1':hour[2:4],'usersql':usersql,'passwordsql':passwordsql,'databasesql':databasesql,'hostsql':hostsql,'portsql':portsql,'task':"move_files_%s"%hour},
	run_as_user='daasuser'
	    )
    alert = EmailOperator(
       task_id='alert_%s'%hour,
       to='{0}'.format(emailReceiver),
       subject='DAAS_Note_MTN_NG_<WBS_hourly> at {0} {1}{2}>'.format(view_date_str,hour[0:2],hour[2:4]),
       html_content='LineCount: {{ task_instance.xcom_pull(key="countline" , task_ids="put_ToDatabase_%s") }} <br>'%hour,
       dag=dag,
       
       
       execution_timeout=timedelta(minutes=1)
       )
    if hour in ['0003','0304','0405','0506']:
        checkHour >> checkTable >> Extraction_PMRated >> gzip_file >> move_files >> put_ToDatabase >> alert
    elif hour == '0001':
        checkHour >> Extraction_PMRated >> gzip_file >> move_files >> put_ToDatabase >> alert >> Dedup
    else:
        checkHour >> Extraction_PMRated >> gzip_file >> move_files >> put_ToDatabase >> alert
