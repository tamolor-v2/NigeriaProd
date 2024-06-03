import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'split',
    'depends_on_past': False,
    'start_date': datetime(2019,10,27),
    'email': ['support@ligadata.com','a.olabamidele@logadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('validation_report_2',
          default_args=default_args,
          schedule_interval=' 0 10 * * *  ',
          catchup=False,
          concurrency=4,
          max_active_runs=1
          )

EBALLOT= BashOperator(
    task_id='EBALLOT',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` EBALLOT.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/NEWREG_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
RDS = BashOperator(
    task_id='RDS',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL RDS.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/RDS_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
group09 = BashOperator(
    task_id='group09',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group09.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/group09_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

INCIDENNT_REPORT = BashOperator(
    task_id='INCIDENNT_REPORT',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL INCIDENNT_REPORT.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/INCIDENNT_REPORT_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group18= BashOperator(
    task_id='group18',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group18.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/group18_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

MAPS= BashOperator(
    task_id='MAPS.conf',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL MAPS.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/MAPS_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

maps_monthly= BashOperator(
    task_id='maps_monthly',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL maps_monthly.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/maps_monthly_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

maps_weekly= BashOperator(
    task_id='maps_weekly',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL maps_weekly.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/maps_weekly_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

maps_2= BashOperator(
    task_id='maps_2',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL maps_2.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/maps_2_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

NETWORK_DAILY= BashOperator(
    task_id='NETWORK_DAILY',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL NETWORK_DAILY.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/NETWORK_DAILY_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

NETWORK_MONTHLY= BashOperator(
    task_id='NETWORK_MONTHLY',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL NETWORK_MONTHLY.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/NETWORK_MONTHLY_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

NETWORK_WEEKLY= BashOperator(
    task_id='NETWORK_WEEKLY',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL NETWORK_WEEKLY.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/NETWORK_WEEKLY_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
EBALLOT>>RDS>>group09>>INCIDENNT_REPORT
group18>>MAPS>>maps_monthly>>maps_weekly
maps_2>>NETWORK_DAILY>>NETWORK_MONTHLY>>NETWORK_WEEKLY

