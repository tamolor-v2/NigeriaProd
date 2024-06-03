from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime,timedelta
import sys


date_param_d0 = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')


def getSubDag_dev5b5(parent_dag_name, child_dag_name, args):
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)


    devd= BashOperator(task_id='devd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_devd daily 9999'.format(date_param_d0) ,      dag=dag,
     run_as_user = 'daasuser')

    simd= BashOperator(task_id='simd',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_simd daily 9999'.format(date_param_d0) ,      dag=dag,
     run_as_user = 'daasuser')

    devm= BashOperator(task_id='devm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_devm daily 9999'.format(date_param_d0) ,      dag=dag,
     run_as_user = 'daasuser')

    devmm= BashOperator(task_id='devmm',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_devmm daily 9999'.format(date_param_d0) ,      dag=dag,
     run_as_user = 'daasuser')
     
    devd >> simd >> devm >> devmm
     
    return dag



