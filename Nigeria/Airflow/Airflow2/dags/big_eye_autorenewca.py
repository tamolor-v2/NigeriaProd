from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from runPresto import presto
from datetime import datetime, timedelta
import logging
from airflow.utils.helpers import chain
import subprocess
from operator import itemgetter

sharedPath = "/nas/share05/sql/templates/be/"
def runCmd(commandToRun):
        '''
        We will be using runCmd in all our calls from the command prompt,
        this will allow us to capture the return code, the output, and the error if
        any exists. We can use this function as follows runCmd(commandString).
        commandString being the bash command we will be running, s_return is the
        return code we get from running said command, s_output is the output of the
        command run, s_err is the error message we get.
        '''
        proc = subprocess.Popen(commandToRun, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err


def uniqueStage(lst):
    #append a list with all the stages    
    unique_stage = []
    for i in lst:
        if i[2] not in unique_stage:
            unique_stage.append(int(i[2]))
    unique_stage.sort()
    print(unique_stage)
    return unique_stage

def mxStgDt(unique_stage,sorted_list):
    max_stg_dt = {}
    for i in unique_stage:
        for j in sorted_list:
            print(max_stg_dt)
            if i == j[2]:
                if str(j[2]) not in max_stg_dt.keys():
                    max_stg_dt.update({str(j[2]):int(j[1])})
                elif str(j[2]) in max_stg_dt.keys():
                    if int(j[1]) > max_stg_dt.get(str(i)):
                        max_stg_dt.update({str(j[2]):int(j[1])})
    return max_stg_dt    


def mainTsk(run_date,files):
    cmdToRun = "ls  {sharedPath}/ | grep '{files}' | grep -v fix | grep -v all | grep -v basic".format(sharedPath=sharedPath,files=files)
    #Lists files in directory without the fix run files
    a,b,c = runCmd(cmdToRun)
    print(b)
    af = (b.decode()).split('\n')
    del af[len(af)-1]
    lst = []
    #append a list with two values [name of file, date of activity,stage]
    for i in af:
        d = (i.split('_')[-1])
        e = d[:8]
        f = (i.split('_')[-2])
        lst.append([i,e,int(f)])
    sorted_list = sorted(lst, key=itemgetter(2,1))
    #lst.sort(key = lambda x:x[0])
    #print("This is the sorted list")
    #print(sorted_list)
    unique_stage = uniqueStage(lst)
    max_stg_dt=mxStgDt(unique_stage,sorted_list)
    ftr = checkRunTrans(run_date, sorted_list,unique_stage,max_stg_dt,files)
    return ftr


def checkRunTrans(run_date, filelist,unique_stage,max_stg_dt,files):
    filesToRun = []
    for stgnum in unique_stage:
        run_flg = 0
        for i in range(0, len(filelist)+1):
            if i < len(filelist)-1:
                if stgnum == filelist[i][2]:
                    if run_flg == 0:
#                        print("stgnum is : " + str(stgnum))
#                        print ("if {rdt} >= {fl} and {rdt} < {fl1} and {stg} == {fls}".format(rdt=run_date,fl=filelist[i][1],fl1=filelist[i+1][1],stg=stgnum,fls=filelist[i][2]))
                        if int(run_date) >= max_stg_dt.get(str(stgnum)):
                            run_flg = 1
                            filesToRun.append(str("{}".format(files) + str(stgnum) + "_" + str(max_stg_dt.get(str(stgnum))) + ".sql"))
                        if int(run_date) >= int(filelist[i][1]) and int(run_date) < int(filelist[i+1][1]) and stgnum == filelist[i][2]:
                            run_flg = 1
                            filesToRun.append(str(filelist[i][0]))
            if i == len(filelist)-1:
                if stgnum == filelist[i][2]:
                    if run_flg == 0:
                        if int(run_date) >= max_stg_dt.get(str(stgnum)):
                            run_flg = 1
                            filesToRun.append(str("{}".format(files) + str(stgnum) + "_" + str(max_stg_dt.get(str(stgnum))) + ".sql"))
                        else :
                            run_flg = 1
                            filesToRun.append(str(filelist[i][0]))
    print(filesToRun)
    return filesToRun


runDt = 20210501
filesToRun = mainTsk(runDt,"be_autorenewca_")

prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'


def readQ(path):
    file = open(path,mode='r')
    readQuery = file.read()
    return str(readQuery).replace("yyyymmdd",datetime.now().strftime('%Y%m%d'))


#prjdQuery = readQ('/nas/share03/airflow/test_trans.sql')
#prjdCheckQuery = 'select count(*) from nigeria.dag_be_projection'
oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)

def excQ(p,q):
    print(q)
    res = p.executeQuery(q,True)
    return str(res[0][0])

def excT(p,q):
    res = p.executeTransaction(q)
    print(res)
    return res

def getSubDag_BigEyeAutorenewca(parent_dag_name, child_dag_name, args):
    escDateKey='escDateKey=True'
    global oPresto 
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)
    run_sql_ = []
    for i in range(0,len(filesToRun)):
        run_sql_.append(PythonOperator(task_id = 'be_autorenewca_' + str(i+1),depends_on_past=False,python_callable=excT,op_args=[oPresto,readQ("{sharedPath}/{ftr}".format(sharedPath=sharedPath,ftr=filesToRun[i]))],dag=dag,run_as_user = 'daasuser'))
        if i not in [0]:
            run_sql_[i-1] >> run_sql_[i]
    print('--------Autorenewca-------')
    print(run_sql_)
    return dag


