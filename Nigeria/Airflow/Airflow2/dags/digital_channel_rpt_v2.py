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
    'email': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')
dag = DAG(
    dag_id='DIGITAL_CHANNEL_RPT_V2',
    default_args=args,
    schedule_interval='0 10 * * *',
    description='DIGITAL_CHANNEL_RPT_V2',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)
get_all_channels = BashOperator(
     task_id='RUN_get_all_channels' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/get_all_data.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
analyze_combined = BashOperator(
     task_id='RUN_analyze_combined' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/All_Digital_Channel.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
analyze_chatbot = BashOperator(
     task_id='RUN_analyze_chatbot' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/ChatBot_Channel.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
analyze_web = BashOperator(
     task_id='RUN_analyze_web' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/MtnWeb_Channel.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
analyze_app = BashOperator(
     task_id='RUN_analyze_app' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/MtnApp_Channel.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
summarize_digital_channels = BashOperator(
     task_id='summarize_digital_channels' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/Summary_Digital_Channel.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
run_d2 = BashOperator(
     task_id='run_d2' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/run_all.sh `date --date="-1 days" +%Y%m%d` '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
run_d3 = BashOperator(
     task_id='run_d3' ,
     bash_command='bash /nas/share05/dataOps_prod/digital_channel_rpt/run_all.sh `date --date="-2 days" +%Y%m%d` '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)

get_all_channels >> [analyze_combined,analyze_chatbot,analyze_web,analyze_app] >> summarize_digital_channels >> run_d2 >> run_d3


