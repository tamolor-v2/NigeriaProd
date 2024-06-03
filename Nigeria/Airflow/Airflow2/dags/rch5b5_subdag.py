from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime,timedelta
import sys


date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def getSubDag_rch5b5(parent_dag_name, child_dag_name, args):
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)


    rchd = BashOperator(task_id='rchd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')

    rchm = BashOperator(task_id='rchm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    rchaggrd = BashOperator(task_id='rchaggrd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchaggrd daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    rchaggrm = BashOperator(task_id='rchaggrm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchaggrm monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    validation_rch = BashOperator(task_id='validation_rch',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_rch daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
     
    
    rchd >> rchm >> rchaggrd >> rchaggrm >> validation_rch
     
    return dag



