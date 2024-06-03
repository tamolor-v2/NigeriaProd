from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime,timedelta
import sys


date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def getSubDag_rev5b5(parent_dag_name, child_dag_name, args):
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)


    revd = BashOperator(task_id='revd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')

    revdd = BashOperator(task_id='revdd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revdd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revm = BashOperator(task_id='revm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revmm = BashOperator(task_id='revmm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revmm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revaggrscd = BashOperator(task_id='revaggrscd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revaggrscm = BashOperator(task_id='revaggrscm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revaggrscdd = BashOperator(task_id='revaggrscdd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscdd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revaggrscmm = BashOperator(task_id='revaggrscmm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscmm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revaggrd = BashOperator(task_id='revaggrd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    revaggrm = BashOperator(task_id='revaggrm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    validation_rev = BashOperator(task_id='validation_rev',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_rev daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    
    revd >> revdd >> revm >> revmm >> revaggrscd >> revaggrscm >> revaggrscdd >> revaggrscmm >> revaggrd >> revaggrm >> validation_rev
     
    return dag



