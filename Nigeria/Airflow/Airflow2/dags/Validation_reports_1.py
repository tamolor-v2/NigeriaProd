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


dag = DAG('validation_report_1',
          default_args=default_args,
          schedule_interval=' 0 10 * * *  ',
          catchup=False,
          concurrency=4,
          max_active_runs=1
          )

NEWREG= BashOperator(
    task_id='NEWREG',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL NEWREG.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/NEWREG_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
kpi_1= BashOperator(
    task_id='kpi_1',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL kpi_1.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/kpi_1_$(date +%Y%m%d%H%M%S).txt ', 
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group08= BashOperator(
    task_id='group08',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group08.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/group08_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

SmartCapex= BashOperator(
    task_id='SmartCapex',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL SmartCapex.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/SmartCapex_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group12= BashOperator(
    task_id='group12',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group12.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/group12_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

GROUP_23= BashOperator(
    task_id='VR_GROUP_23',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL GROUP_23.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/GROUP_23_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

kpi_2= BashOperator(
    task_id='kpi_2',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL kpi_2.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/kpi_2_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group10= BashOperator(
    task_id='group10',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group10.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/group10_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

maps_daily= BashOperator(
    task_id='maps_daily',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL maps_daily.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/maps_daily_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

ValidationTool_Non_BSL= BashOperator(
    task_id='ValidationTool_Non_BSL',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL ValidationTool_Non_BSL.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/ValidationTool_Non_BSL_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)


CS5= BashOperator(
    task_id='CS5',
    bash_command='/nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL CS5.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/CS5_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)


group38= BashOperator(
    task_id='group38',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group38.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/group38_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
NEWREG>>kpi_1>>group08>>SmartCapex
group12>>kpi_2>>GROUP_23>>group10
maps_daily>>ValidationTool_Non_BSL>>CS5>>group38
