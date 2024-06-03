from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','ayodeji.shadare@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

d_0 = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')
d_1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
d_2 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
d_3 = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
d_4 = (datetime.now() - timedelta(days=4)).strftime('%Y%m%d')
d_5 = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
d_6 = (datetime.now() - timedelta(days=6)).strftime('%Y%m%d')
d_7 = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
d_8 = (datetime.now() - timedelta(days=8)).strftime('%Y%m%d')
d_9 = (datetime.now() - timedelta(days=9)).strftime('%Y%m%d')
d_10 = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')

dag = DAG(
    dag_id='extract_wbs_report',
    default_args=args,
    #schedule_interval='0 4 * * * ',
    schedule_interval=None,
    catchup=False,
    concurrency=5,
    max_active_runs=5
)

start = BashOperator(
     task_id='start' ,
     bash_command='echo start',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_1 = BashOperator(
     task_id='extract_wbs_report_1' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_2 = BashOperator(
     task_id='extract_wbs_report_2' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_2),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_3 = BashOperator(
     task_id='extract_wbs_report_3' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_3),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_4 = BashOperator(
     task_id='extract_wbs_report_4' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_4),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_5 = BashOperator(
     task_id='extract_wbs_report_5' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_5),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_6 = BashOperator(
     task_id='extract_wbs_report_6' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_6),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_7 = BashOperator(
     task_id='extract_wbs_report_7' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_7),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_8 = BashOperator(
     task_id='extract_wbs_report_8' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_8),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_9 = BashOperator(
     task_id='extract_wbs_report_9' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_9),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_10 = BashOperator(
     task_id='extract_wbs_report_10' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_10),
     dag=dag,
     run_as_user = 'daasuser'
)

extract_wbs_report_0 = BashOperator(
     task_id='extract_wbs_report_0' ,
     bash_command='bash /nas/share05/ops/daily/wbs_report.sh {0} '.format(d_0),
     dag=dag,
     run_as_user = 'daasuser'
) 

end = BashOperator(
     task_id='end' ,
     bash_command='echo end',
     dag=dag,
     run_as_user = 'daasuser'
)

start >> [extract_wbs_report_1,extract_wbs_report_2, extract_wbs_report_3, extract_wbs_report_4, extract_wbs_report_5, extract_wbs_report_6, extract_wbs_report_7, extract_wbs_report_8, extract_wbs_report_9, extract_wbs_report_10, extract_wbs_report_0 ] >> end
