from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime,timedelta
import sys


date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def getSubDag_usg5b5(parent_dag_name, child_dag_name, args):
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)


    usgd = BashOperator(task_id='usgd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')

    usgdd = BashOperator(task_id='usgdd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgdd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgm = BashOperator(task_id='usgm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgmm = BashOperator(task_id='usgmm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgmm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgaggrscd = BashOperator(task_id='usgaggrscd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgaggrscm = BashOperator(task_id='usgaggrscm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgaggrscdd = BashOperator(task_id='usgaggrscdd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscdd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgaggrscmm = BashOperator(task_id='usgaggrscmm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscmm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgaggrd = BashOperator(task_id='usgaggrd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    usgaggrm = BashOperator(task_id='usgaggrm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    validation_usg = BashOperator(task_id='validation_usg',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_usg daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    
    usgd >> usgdd >> usgm >> usgmm >> usgaggrscd >> usgaggrscm >> usgaggrscdd >> usgaggrscmm >> usgaggrd >> usgaggrm >> validation_usg
     
    return dag



