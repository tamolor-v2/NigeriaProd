from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from runPresto import presto
import logging

prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'

def readQ(path):
    file = open(path,mode='r')
    readQuery = file.read()
    return readQuery.format(yyyymmdd = datetime.now().strftime('%Y%m%d'))

dataccpre1Query = 'select 1'
datacdQuery = 'select 1'
datacdaQuery = 'select 1'
dataccpre2Query = 'select 1'
autorenewcdQuery = 'select 1'
autorenewcmQuery = 'select 1'
autorenewcqQuery = 'select 1'
autorenewcyQuery = 'select 1'
autorenewcaQuery = 'select 1'
datapldQuery = 'select 1'
datagdQuery = 'select 1'
datagdaQuery = 'select 1'
datagdaaaQuery = 'select 1'
devdQuery = 'select 1'
ebudQuery = 'select 1'
monthlyCubeQuery = 'select 1'
dailyCubeQuery = 'select 1'
dataccmaQuery = 'select 1'
dataccdaQuery = 'select 1'
dataCheckQuery = 'select 1'

oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)


def getSubDag_BigEyeData(parent_dag_name, child_dag_name, args):
    global oPresto
    global dataccpre1Query
    global datacdQuery
    global datacdaQuery
    global dataccpre2Query
    global autorenewcdQuery
    global autorenewcmQuery
    global autorenewcqQuery
    global autorenewcyQuery
    global autorenewcaQuery
    global datapldQuery
    global datagdQuery
    global datagdaQuery
    global datagdaaaQuery
    global devdQuery
    global ebudQuery
    global monthlyCubeQuery
    global dailyCubeQuery
    global dataccmaQuery
    global dataccdaQuery
    global dataCheckQuery
    escDateKey=True
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)
    dataccpre1 = PythonOperator(task_id='dataccpre1', python_callable=oPresto.executeTransaction,op_args=[dataccpre1Query], dag=dag, run_as_user = 'daasuser')
    datacd = PythonOperator(task_id='datacd', python_callable=oPresto.executeTransaction,op_args=[datacdQuery], dag=dag, run_as_user = 'daasuser')
    datacda = PythonOperator(task_id='datacda', python_callable=oPresto.executeTransaction,op_args=[datacdaQuery], dag=dag, run_as_user = 'daasuser')
    dataccpre2 = PythonOperator(task_id='dataccpre2', python_callable=oPresto.executeTransaction,op_args=[dataccpre2Query], dag=dag, run_as_user = 'daasuser')
    autorenewcd = PythonOperator(task_id='autorenewcd', python_callable=oPresto.executeTransaction,op_args=[autorenewcdQuery], dag=dag, run_as_user = 'daasuser')
    autorenewcm = PythonOperator(task_id='autorenewcm', python_callable=oPresto.executeTransaction,op_args=[autorenewcmQuery], dag=dag, run_as_user = 'daasuser')
    autorenewcq = PythonOperator(task_id='autorenewcq', python_callable=oPresto.executeTransaction,op_args=[autorenewcqQuery], dag=dag, run_as_user = 'daasuser')
    autorenewcy = PythonOperator(task_id='autorenewcy', python_callable=oPresto.executeTransaction,op_args=[autorenewcyQuery], dag=dag, run_as_user = 'daasuser')
    autorenewca = PythonOperator(task_id='autorenewca', python_callable=oPresto.executeTransaction,op_args=[autorenewcaQuery], dag=dag, run_as_user = 'daasuser')
    datapld = PythonOperator(task_id='datapld', python_callable=oPresto.executeTransaction,op_args=[datapldQuery], dag=dag, run_as_user = 'daasuser')
    datagd = PythonOperator(task_id='datagd', python_callable=oPresto.executeTransaction,op_args=[datagdQuery], dag=dag, run_as_user = 'daasuser')
    datagda = PythonOperator(task_id='datagda', python_callable=oPresto.executeTransaction,op_args=[datagdaQuery], dag=dag, run_as_user = 'daasuser')
    datagdaaa = PythonOperator(task_id='datagdaaa', python_callable=oPresto.executeTransaction,op_args=[datagdaaaQuery], dag=dag, run_as_user = 'daasuser')
    devd = PythonOperator(task_id='devd', python_callable=oPresto.executeTransaction,op_args=[devdQuery], dag=dag, run_as_user = 'daasuser')
    ebud = PythonOperator(task_id='ebud', python_callable=oPresto.executeTransaction,op_args=[ebudQuery], dag=dag, run_as_user = 'daasuser')
    monthlyCube = PythonOperator(task_id='monthlyCube', python_callable=oPresto.executeTransaction,op_args=[monthlyCubeQuery], dag=dag, run_as_user = 'daasuser')
    dailyCube = PythonOperator(task_id='dailyCube', python_callable=oPresto.executeTransaction,op_args=[dailyCubeQuery], dag=dag, run_as_user = 'daasuser')
    dataccma = PythonOperator(task_id='dataccma', python_callable=oPresto.executeTransaction,op_args=[dataccmaQuery], dag=dag, run_as_user = 'daasuser')
    dataccda = PythonOperator(task_id='dataccda', python_callable=oPresto.executeTransaction,op_args=[dataccdaQuery], dag=dag, run_as_user = 'daasuser')
    dataCheck = PythonOperator(task_id='dataCheck', python_callable=oPresto.executeTransaction,op_args=[dataCheckQuery], dag=dag, run_as_user = 'daasuser')
    datacd >> datacda
    datacda >> dataccpre2
    datacda >> autorenewcd
    datacda >> autorenewcm
    datacda >> autorenewcq
    datacda >> autorenewcy
    datacda >> autorenewca
    datacda >> datapld
    datagd >> datagda >> datagdaaa
    datagd >> datagda >> devd
    [dataccpre1,dataccpre2] >> monthlyCube >> dataccma
    [dataccpre1,dataccpre2] >> dailyCube >> dataccda

    [autorenewcd,autorenewcm,autorenewcq,autorenewcy,autorenewca,datapld,datagdaaa,devd,ebud,dataccma,dataccda] >> dataCheck
    return dag

