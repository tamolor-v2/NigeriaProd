from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from runPresto import presto
from datetime import datetime, timedelta

import logging

prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'
'''
#read query and replace yyyymmdd with date from runid
def readQ(path):
    file = open(path,mode='r')
    readQuery = file.read()
    return readQuery.format(yyyymmdd = datetime.now().strftime('%Y%m%d'))

#
evtdQuery= 'select 1'
subdQuery= readQ('/nas/share05/sql/templates/be/be_subd.sql')
subddQuery= readQ('/nas/share05/sql/templates/be/be_subdd.sql')
subdaQuery= readQ('/nas/share05/sql/templates/be/be_subda.sql')
subcheckQuery= readQ('/nas/share05/sql/templates/be/be_subd_validate.sql')


oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)
escDateKey=True

def getSubDag_BigEyeSubscriber(parent_dag_name, child_dag_name, args):
    global oPresto
    global evtdQuery
    global subdQuery
    global subddQuery
    global subdaQuery
    global subcheckQuery
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)
    evtd = PythonOperator(task_id='evtd', python_callable=oPresto.executeTransaction,op_args=[evtdQuery], dag=dag, run_as_user = 'daasuser')
    subd = PythonOperator(task_id='subd', python_callable=oPresto.executeTransaction,op_args=[subdQuery], dag=dag, run_as_user = 'daasuser')
    subdd = PythonOperator(task_id='subdd', python_callable=oPresto.executeTransaction,op_args=[subddQuery], dag=dag, run_as_user = 'daasuser')
    subda = PythonOperator(task_id='subda', python_callable=oPresto.executeTransaction,op_args=[subdaQuery], dag=dag, run_as_user = 'daasuser')
    subcheck = PythonOperator(task_id='subcheck',python_callable=oPresto.executeQuery,op_args=[subcheckQuery,escDateKey],dag=dag,run_as_user = 'daasuser')
    evtd >> subd >> subdd >> subda >> subcheck
    return dag
'''
