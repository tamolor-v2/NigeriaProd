# this template is for creating airflows with no dependencies i.e the task does not depend another task to run first
# follow the steps commented out everything you do in airflow should be on Node 149
# step (1) the first thing you want to do is navigate into this directory /usr/lib/python3.6/site-packages/airflow/example_dags using cd.
# step (2) The next thing to do is copy the script name with zerodependency.py and rename to the new airflow name you want
# e.g if I want to create a new airflow with name upgraded4g.py I will do (cp zerodependency.txt upgraded4g.py), what this does is create a #new copy of # zerodependency.py with new name upgraded4g.py. After this you are ready to start editing your own template.

# step (3) Open the text editor to edit your new scipt using vi dagname.py, for the example above I will do (vi upgraded4g.py)
# This script has been divided into sections so just follow the documentation for each section.



#MODULE IMPORT SECTION : This section is importing some modules needed for this script to run. just leave it as it is.
########################################################################################################################
from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import os

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

########################################################################################################################
#ARGS SECTION: just leave this section as it is and move on!
########################################################################################################################

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

########################################################################################################################
#DAG SECTION
########################################################################################################################
#(1) Change the value of the dag_id to one that explains or references what the report is about e.g if the airflow task is about 4gupgraded use something like 4gupgraded_reports.
#(2)leave the default_args as it is.
#(3)You can put a description of the report in the description section.
#(4)Schedule_interval is where we decide the intervals with which the report or task will be ran. the method we are using follows crontab jo#b scheduling method so you can just google "how to schedule for (your time interval) in crontabjob you will see something like(*/30 * * * *# *) copy this into the schedule_interval (https://crontab.guru/)
#(5)leave concurrency as it is.
#(6)leave catchup as it is.
#(7)leave max_active_runs as it is.
########################################################################################################################

dag = DAG(
    dag_id='NB_summary_DAILY_RECON',
    default_args=args,
    schedule_interval='0 8,14 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)


########################################################################################################################
#TASK SECTION: this is the section where we specify the task we want to run.
########################################################################################################################
#(1)task_id: change the task id to a value that can describe what the report is about.
#(2)bash_command: this part can be tricky but dont fear ask for the right value to input here from The two Mr olanipekun this is because the# value here varies depending on how the task should run but its not that deep so just ask!.
#(3)dag: just leave this section as it is.
########################################################################################################################

t1 = BashOperator(
     task_id='4gupgraded' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p DAILY_RECON -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)

t1

