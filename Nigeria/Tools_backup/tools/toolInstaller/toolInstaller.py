#- input toolName,
#PCF, VR, Dedup, feedExtractor, RunTransaction, AuditingTool
#- config location

#1 airflow import global variable

from datetime import datetime, timedelta
import os,sys,re
import subprocess as s
import csv , json , gzip
import argparse
import logging
import tarfile

#A function to untar the tools
def untarTool(toolDir):
    try:
        if('tar.gz' in toolDir):
            logging.info("Extracting the tools..")
            dir = os.path.dirname(toolDir) # get directory where file is stored
            filename =  os.path.basename(toolDir) # get filename
            os.chdir(dir)
            tar = tarfile.open(filename)
            #print(filename)
            tar.extractall(dir)
            tar.close()
        logging.info("Extracting the tools done!")
    except Exception as e:
        logging.error("Extracting the tools Failed due to: {0}".format(str(e)))
        exit()

# A function to get the server time
def getTime():
    return (datetime.today()).strftime('%Y%m%d%H%M%S')

# A function to get the server time with HH:mm:SS format
def getSysTime():
    return (datetime.today()).strftime('%H:%M:%S')

# A function to get the server date
def getServerDate():
    return (datetime.today()).strftime('%Y%m%d')

# A function to read Json config file and return the result as a map
def readJsonFile(path):
    data = json.loads(open(path).read())
    return data

# A function to show what are the parsed args that the user should use
def getArgs():
    parser = argparse.ArgumentParser(prog='parsePlotSens')
    parser.add_argument('-c','--configFile', help='',required = True)
    parser.add_argument('-t','--toolName', help='',required = True)
    return parser.parse_args()

#A function to run the shell commands
def run_cmd(command):
    proc = s.Popen(command, shell=True, stdout=s.PIPE, stderr=s.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, str(s_output).replace("b'","").replace("/n'",""), s_err

#A function to import the global variables into airflow
def importAirflowGlobalVar(airflowNode,airflowFile):
    try:
        import paramiko
        try:
            logging.info("Connecting to the Airflow node - {0}".format(airflowNode))
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(airflowNode)
        except paramiko.AuthenticationException:
            logging.error("Authentication failed, please verify your credentials!")
            exit()
        except paramiko.SSHException as sshException:
            logging.error("Unable to establish SSH connection!")
            exit()
        except paramiko.BadHostKeyException as badHostKeyException:
            logging.error("Unable to verify server's host key!")
            exit()
        except:
            logging.error("Unable to establish SSH connection")
            exit()
        logging.info("Importing the global variables.. ")
        cmd = "airflow variables import {0}".format(airflowFile)
        ssh.exec_command(cmd) #run_cmd("airflow variables import {0}".format(airflowFile))
    except Exception as e:
        logging.error("Importing the global variables failed due to: {0}".format(str(e)))
        exit()

# A function to replace the values inside the bin with the config file values
def replaceBinParam(basePath,toolOption,hdfsConfigLocation=None,kamanjaLibLocation=None,dagsLocation=None,logLocation=None,prestoConf=None):
    try:
        binArr=[]
        fullBaseDir = "{0}/{1}/bin".format(basePath,toolOption)
        #loop on the files to check if the listed dirs. are files
        for binFile in os.listdir(fullBaseDir):
            if(os.path.isfile(os.path.join(fullBaseDir, binFile))):
                binArr.append(binFile)
        logging.info("Working on the below files:\n {0}".format(binArr))
        os.chdir(fullBaseDir)
        #loop on the array of files to replace the needed variables
        for iFile in binArr:
            logging.info("Working on {0}".format(iFile))
            #read the file
            inFile=open(iFile,"rt")
            dataReplacement=inFile.read()
            dataReplacement=dataReplacement.replace('$basePath',basePath).replace('$hdfsConfigPath',hdfsConfigLocation).replace('$kamanjaPath',kamanjaLibLocation).replace('$dagPath',dagsLocation).replace('$logPath',logLocation).replace("$prestoConfig",prestoConf)
            inFile.close()
            #write into the file
            inFile=open(iFile,"wt")
            inFile.write(dataReplacement)
            inFile.close()
            logging.info("Done working on {0}".format(iFile))
    except Exception as e:
        logging.error("Replacing the values for the bin files failed due to: {0}".format(str(e)))
        exit()

# A function to create the base dirs if not exists
def mkBaseDirs(basePath,toolName,toolNameArr=None):
    try:
       # checking if the dirs. are exist
        logsBaseDir= os.path.isdir(basePath+'/logs')
        tempBaseDir= os.path.isdir(basePath+'/tmp')
        jarsBaseDir= os.path.isdir(basePath+'/JARs')
        if (logsBaseDir == False ):
            logging.info("Creating the logs base path.. ")
            run_cmd('mkdir {0}/logs'.format(basePath))
        if(tempBaseDir == False):
            logging.info("Creating the tmp base path..")
            run_cmd('mkdir {0}/tmp'.format(basePath))
        if(jarsBaseDir == False):
            logging.info("Creating the JARs base path..")
            run_cmd('mkdir {0}/JARs'.format(basePath))
        else:
            logging.info("The base paths already exist")

        # checking if the option is ALL or multi feeds parsed
        if(toolName.lower()=='all' or (toolName in ',')):
            for i in toolNameArr:
                logging.info("Creating the {0} tool configs path if not exists..".format(i))
                run_cmd('mkdir -p {0}/{1}/conf'.format(basePath,i))
                logging.info("Creating the {0} tool bin path if not exists..".format(i))
                run_cmd('mkdir -p {0}/{1}/bin'.format(basePath,i))
                if(i.lower()=='vr'):
                    logging.info("Creating the {0} tool templates path if not exists..".format(i))
                    run_cmd('mkdir -p {0}/{1}/templates/'.format(basePath,i))
        else:
            logging.info("Creating the {0} tool config path if not exists..".format(toolName))
            run_cmd('mkdir -p {0}/{1}/conf'.format(basePath,toolName))
            logging.info("Creating the {0} tool bin path if not exists..".format(toolName))
            run_cmd('mkdir -p {0}/{1}/bin'.format(basePath,toolName))
            if(toolName.lower()=='vr'):
                logging.info("Creating the {0} tool templates path if not exists..".format(toolName))
                run_cmd('mkdir -p {0}/{1}/templates/'.format(basePath,toolName))

    except Exception as e:
        logging.error("Making the directories failed due to: {0}".format(str(e)))
        exit()

# A function to copy the requirements
def cpRequirement(basePath,toolPath,toolName,toolNameArr,dagsLocation=None):
    try:
        if(toolName.lower() =='all' or (toolName in ',')):
            logging.info("Copying the tools JARs started..")
            for i in toolNameArr:
                #copying the JARs
                run_cmd('cp {1}/{2}/JARs/* {0}/JARs/'.format(basePath,toolPath,i))
                logging.info("Copying the {0} tool JARs..".format(i))
                #copying the bin
                run_cmd('cp {1}/{2}/bin/* {0}/{2}/bin/'.format(basePath,toolPath,i))
                logging.info("Copying the {0} tool bin directory..".format(i))
                #copying the configs
                run_cmd('cp {1}/{2}/conf/* {0}/{2}/conf/'.format(basePath,toolPath,i))
                logging.info("Copying the {0} tool conf directory..".format(i))
                if(i.lower()=='vr'):
                    #copying the template
                    run_cmd('cp {1}/{2}/templates/* {0}/{2}/templates/'.format(basePath,toolPath,i))
                    logging.info("Copying the {0} tool template directory..".format(i))
            logging.info("Copying the Airflow global functions..")
            run_cmd('cp {1}/Airflow/* {0}'.format(dagsLocation,toolPath))
        elif (toolName.lower()=='airflow'):
            logging.info("Copying the Airflow global functions..")
            run_cmd('cp {1}/Airflow/* {0}'.format(dagsLocation,toolPath,i))
        else:
            #copying the JARs
            run_cmd('cp {1}/{2}/JARs/* {0}/JARs/'.format(basePath,toolPath,toolName))
            logging.info("Copying the {0} tool JARs..".format(toolName))
            #copying the bin
            run_cmd('cp {1}/{2}/bin/* {0}/{2}/bin/'.format(basePath,toolPath,toolName))
            logging.info("Copying the {0} tool bin directory..".format(toolName))
            if(toolName.lower()=='vr'):
                #copying the template
                run_cmd('cp {1}/{2}/templates/* {0}/{2}/templates/'.format(basePath,toolPath,toolName))
                logging.info("Copying the {0} tool template directory..".format(toolName))
    except Exception as e:
        logging.error("Copying the requirement files failed due to: {0}".format(str(e)))
        exit()

def main() :
    #get args
    args=getArgs()
    configData = readJsonFile(args.configFile)
    toolName = args.toolName

    #get the config general variables
    paths = configData["Locations"]
    tools = configData["Tools"]
    configs = configData["Configs"]
    toolsArr=[]

    #create the log file
    toolLogPath = "{0}/{1}/".format(paths["logLocation"],getServerDate())
    toolLogFile = "{0}/{1}_{2}.log".format(toolLogPath,toolName,getTime())
    run_cmd("mkdir -p {0}".format(toolLogPath))

    #config the log
    level=logging.INFO
    FORMAT = '%(levelname)s %(asctime)s - %(message)s'
    formatter = logging.Formatter(FORMAT)

    #init logger
    logger_name=""
    log = logging.getLogger(logger_name)
    log.setLevel(level)

    #logger to writing on file
    fh = logging.FileHandler(toolLogFile, mode='w', encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    #logger to print into screen
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    log.info("The extraction job started..")
    log.info("Log file -- {0}".format(toolLogFile))

    #fill the tools array
    for i in range(len(tools)):
        toolsArr.append((tools[i]["name"]))
    #check if the passed tool is in the tools list at the config file
    if(toolName.lower() =='all'):
        log.info("Working on {0} tools..".format(toolName.upper()))
    elif(toolName not in toolsArr):
        log.error("{0} tool doesn't exist at the config file!".format(toolName))
        exit()
    else:
        log.info("Working on {0} tool..".format(toolName))

    #check if all of the needed paths declared at the config file
    if( str(configData).find("baseLocation")== -1 or str(configData).find("toolLocation")== -1 or str(configData).find("dagsLocation")== -1 or str(configData).find("hdfsConfigLocation")== -1 or str(configData).find("kamanjaLibLocation")== -1 ):
        log.error("One or more of the needed paths missing at the config file!")
        log.error("The needed paths are: baseLocation ,toolLocation ,dagsLocation ,hdfsConfigLocation ,kamanjaLibLocation")
        exit()

    #get the config values
    basePath = paths["baseLocation"]
    toolPath = paths["toolLocation"]
    dagsPath = paths["dagsLocation"]
    hdfsPath= paths["hdfsConfigLocation"]
    kamanjaPath=paths["kamanjaLibLocation"]
    prestoConf=configs["presto"]
    airflowNode=configs["airflowNode"]
    airflowFile=paths["airflowLocation"]

#1 untar the tools
    untarTool(toolPath)
#2 create the base dirs.
    mkBaseDirs(basePath,toolName,toolsArr)
#3 copy the JARs,bin to each tool & #5 import the airflow global functions to the airflow
    if(toolName.lower()=='airflow' or toolName.lower()=='all'):
        cpRequirement(basePath,toolPath.replace('.tar.gz',''),toolName,toolsArr,dagsPath)
        importAirflowGlobalVar(airflowNode,airflowFile)
    else:
        cpRequirement(basePath,toolPath.replace('.tar.gz',''),toolName,toolsArr)
#4 variables replacement
    replaceBinTools = ['PCF','Dedup','AuditingTool']
    if(toolName.lower()=='all'):
        for iTool in replaceBinTools:
            replaceBinParam(basePath,iTool,hdfsPath,kamanjaPath,dagsPath,paths["logLocation"],prestoConf)
    if(toolName.lower()=='pcf' or toolName.lower()=='dedup' or toolName.lower()=='auditingtool'):
        replaceBinParam(basePath,toolName,hdfsPath,kamanjaPath,dagsPath,paths["logLocation"],prestoConf)



if __name__ == "__main__" :
    main()
    