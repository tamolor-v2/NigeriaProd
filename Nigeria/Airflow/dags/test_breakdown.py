from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, EmailOperator, PythonOperator
import json, sys, os



##time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=/mnt/beegfs/tools/tarringscript/log4j2.xml /mnt/beegfs/tools/fileOps_listing/FileOps-1.0.1.200119.jar  -in {0}  -nt 30 -out {1} -dp 50 -tf 1 -lbl eva_breackdown -of -od -hdfs {2} -kktf "/etc/security/keytabs/daasuser.keytab" -kp "daasuser@MTN.COM" -fromHdfs -op listing##
FileOps_cmd = Variable.get("breakdown_FileOps_cmd", deserialize_json=True)
HDFS_report_path = Variable.get("breakdown_HDFS_report_path", deserialize_json=True)
Hive = Variable.get("breakdown_hive_con", deserialize_json=True)
report_path = Variable.get("breakdown_report_path", deserialize_json=True)
HDFS_root_path = Variable.get("breakdown_HDFS_root_path", deserialize_json=True)
HDFS_paths = Variable.get("breakdown_HDFS_paths", deserialize_json=True)
schema = Variable.get("breakdown_schema", deserialize_json=True)
table = Variable.get("breakdown_table", deserialize_json=True)


#FileOps_cmd = 'time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=/mnt/beegfs_bsl/ashraf/breakdown/log4j2.xml /mnt/beegfs_bsl/ashraf/breakdown/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in {0}  -nt 30 -out {1} -dp 50 -tf 1 -lbl eva_breackdown -of -od -hdfs {2} -kktf "/etc/security/keytabs/daasuser.keytab" -kp "daasuser@MTN.COM" -fromHdfs -op listing'
#HDFS_report_path = "/test1"
#Hive = "hive "
#report_path = "/mnt/beegfs_bsl/ashraf/breakdown/"
#HDFS_root_path = "hdfs://ngdaas"
#HDFS_paths = "/test1"
#schema = "audit"
#table = "breakdown"


create_table_statment = '''CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (seq int,l1dirs double,l1files double,lxdirs double,lxfiles double,l1filessizeinb double,lxfilessizeinb double,l1gzfiles string,lxgzfiles string,linecount double,uncompressedsize double,level int,lxdirdurationinmicros double,lxfiledurationinmicros double,path string) PARTITIONED BY (tbl_dt int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '{2}' TBLPROPERTIES ( 'transient_lastDdlTime'='1593511389')'''.format(schema, table, "%s/%s"%(HDFS_root_path, HDFS_report_path))



 
today =  datetime.today()
todayStr = today.strftime('%Y%m%d')



args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,3,20),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('HDFS_breakdown',
    default_args=args,
    schedule_interval= " 0 3 * * 6,2 ",
	catchup=False,
    max_active_runs=1
          )

def create_partition():
    check = os.system("hadoop fs -test -e %s/tbl_dt=%s"%(HDFS_report_path, todayStr))
    if check is not "0":
        try:
            check = os.system("hadoop fs -mkdir -p %s/tbl_dt=%s"%(HDFS_report_path, todayStr))
            if check is not "0":
                exit(1)
        except:
            print("not able to create partition: %s/tbl_dt=%s"%(HDFS_report_path, todayStr))
    else:
        print("partition is already exist")

def create_report_path():
    if not os.path.exists(report_path):
        os.makedirs(report_path)
    else:
        print("directory is already exist")

create_report_path = PythonOperator(
    task_id='create_report_path',
    python_callable=create_report_path,
    dag=dag,
    run_as_user='daasuser'
)


FileOps_listing = BashOperator(
    task_id='FileOps_listing',
    bash_command=FileOps_cmd.format(HDFS_paths, report_path, HDFS_root_path),
    dag=dag,
    run_as_user='daasuser'
) 


create_partition = PythonOperator(
    task_id='create_partition',
    python_callable=create_partition,
    dag=dag,
    run_as_user='daasuser'
)



put_report_into_HDFS = BashOperator(
    task_id='put_report_into_HDFS',
    bash_command="hadoop fs -put %s/latest/dirinfo* %s/tbl_dt=%s"%(report_path, HDFS_report_path, todayStr),
    dag=dag,
    run_as_user='daasuser'
) 


create_table = BashOperator(
    task_id='create_table',
    bash_command="%s -e \"%s\""%(Hive, create_table_statment),
    dag=dag,
    run_as_user='daasuser'
) 



msck_table = BashOperator(
    task_id='msck_table',
    bash_command="%s -e 'msck repair table %s.%s'"%(Hive, schema, table),
    dag=dag,
    run_as_user='daasuser'
) 


emails = ""
alert = EmailOperator(
    task_id='alert',
    to='{0}'.format(emails),
    subject='Airflow HDFS_BREAKDOWN',
    html_content='Finish the breakdown_job has been finished and wrote the report in the new tbl_dt partition: %s'%todayStr,
    dag=dag,
    ) 

create_report_path >> create_table >> FileOps_listing >> create_partition >> put_report_into_HDFS >> msck_table >> alert
