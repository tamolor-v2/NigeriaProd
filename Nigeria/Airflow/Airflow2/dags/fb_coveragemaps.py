from __future__ import print_function

from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen
import os
from airflow.operators.python_operator import BranchPythonOperator

day_run = datetime.today()
today_date = datetime.today() - timedelta(days=0)
first_date= today_date.strftime('%Y%m%d')
incoming_path='/mnt/beegfs_api/live/COVERAGE_MAPS/incoming'
tool_location='/nas/share05/tools/Facebook_reports/rename_COVERAGEMAPS.sh'
to_path='/nas/share05/archived/COVERAGE_MAPS/'
#LZ_to_path='/data/data_lz/beegfs/GC_REPORTS/COVERAGE_MAPS/'
LZ_to_path='/ftpout/FACEBOOK/MTNNG_EVA/'

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': datetime(2022,6,1),
    'email': ['m.nabeel@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='FB_COVERAGEMAPS',
    default_args=args,
    schedule_interval= " 0 5 * * * ",
    catchup=False,
    concurrency=3,
    max_active_runs=1)

creating_archive_folder= BashOperator(
     task_id='creating_archive_folder' ,
     depends_on_past=False,
     bash_command= ' mkdir -p {0}/{1} '.format(to_path,first_date),
     dag=dag,
     priority_weight = 10,
     run_as_user = 'daasuser'
)

def branch_check_data_fb_coveragemaps():
    path_1 = os.popen(f'ls -l {incoming_path}/* | grep -i  "shx\|shp\|prj\|dbf" | grep -i "2g\|3g\|4g" | wc -l ').read()
    output = int(path_1)
    if (output == 0 ):
        return 'no_files_to_work_on'
    else:
        return 'creating_archive_folder' #working_on_FB_COVERAGEMAPS'
        
no_files_to_work_on= BashOperator(
     task_id='no_files_to_work_on',
     depends_on_past=False,
     bash_command= ' echo "No files in incoming to work on! " ' ,
     dag=dag,
     run_as_user = 'daasuser'
)

working_on_FB_COVERAGEMAPS= BashOperator(
     task_id='working_on_FB_COVERAGEMAPS' ,
     depends_on_past=False,
     bash_command= ' echo "Working on the files at the incoming.. " ',
     dag=dag,
     run_as_user = 'daasuser'
)

extens= [
"shx"
,"shp"
,"prj" 
,"dbf"
]

run_extens_2g = []
for exten in extens:
     run_extens_2g.append(BashOperator(
     task_id='FB_COVERAGEMAPS_RENAME_2G_'+exten.lower() ,
     depends_on_past=False,
     bash_command='bash {0} --date {1} --path_from {2} --path_to {3} --file_name facebook_fbc_nwi.DSD.raw_fb_mtn_2G_Coverage_ --type_name {4} --Old_name 2G ng '.format(tool_location,first_date,incoming_path,to_path+"/"+first_date,exten),
     dag=dag,
     run_as_user = 'daasuser'
))

run_extens_3g = []
for exten in extens:
     run_extens_3g.append(BashOperator(
     task_id='FB_COVERAGEMAPS_RENAME_3G_'+exten.lower() ,
     depends_on_past=False,
     bash_command='bash {0} --date {1} --path_from {2} --path_to {3} --file_name facebook_fbc_nwi.DSD.raw_fb_mtn_3G_Coverage_ --type_name {4} --Old_name 3G ng '.format(tool_location,first_date,incoming_path,to_path+"/"+first_date,exten),
     dag=dag,
     run_as_user = 'daasuser'
))

run_extens_4g = []
for exten in extens:
     run_extens_4g.append(BashOperator(
     task_id='FB_COVERAGEMAPS_RENAME_4G_'+exten ,
     depends_on_past=False,
     bash_command='bash {0} --date {1} --path_from {2} --path_to {3} --file_name facebook_fbc_nwi.DSD.raw_fb_mtn_4G_Coverage_ --type_name {4} --Old_name 4G ng '.format(tool_location,first_date,incoming_path,to_path+"/"+first_date,exten),
     dag=dag,
     run_as_user = 'daasuser'
))

branch_check_data = BranchPythonOperator(
    task_id='branch_check_data',
    python_callable=branch_check_data_fb_coveragemaps,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

moving_to_LZ= BashOperator(
     task_id='moving_to_LZ' ,
     depends_on_past=False,
     bash_command= ' scp {0}/* edge01002:{1} '.format(to_path+"/"+first_date,LZ_to_path),
     dag=dag,
     run_as_user = 'daasuser'
)


branch_check_data >> creating_archive_folder
branch_check_data >> no_files_to_work_on
creating_archive_folder >> working_on_FB_COVERAGEMAPS
working_on_FB_COVERAGEMAPS >> [run_extens_4g[0], run_extens_4g[1], run_extens_4g[2], run_extens_4g[3], run_extens_3g[0], run_extens_3g[1], run_extens_3g[2], run_extens_3g[3], run_extens_2g[0], run_extens_2g[1], run_extens_2g[2], run_extens_2g[3]]
[run_extens_4g[0], run_extens_4g[1], run_extens_4g[2], run_extens_4g[3], run_extens_3g[0], run_extens_3g[1], run_extens_3g[2], run_extens_3g[3], run_extens_2g[0], run_extens_2g[1], run_extens_2g[2], run_extens_2g[3]] >> moving_to_LZ 
