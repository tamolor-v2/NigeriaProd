from datetime import datetime, timedelta
import os,sys
import subprocess as s
import re
from shutil import copyfile
import threading
import csv , json , time, gzip
from collections import deque
import argparse
import logging
import urllib
from bson.json_util import dumps

import pandas as pnd

import cx_Oracle                 # used to connect to Oracle DB
import mysql.connector as Myconn # used to connect to MySQL DB
import pyodbc  as pd             # used to connect to SqlServer
from pymongo import MongoClient  # used to connect to MongoDB

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
    parser.add_argument('-c','--configFile', help='configFile',required = True)
    parser.add_argument('-f','--feedName', help='feedName',required = True)
    parser.add_argument('-d','--extractionDate', help='The date you want to extract',required = True)
    parser.add_argument('-sh','--startHour', help='The start hour you want to extract',required = False)
    parser.add_argument('-eh','--endHour', help='The end hour you want to extract',required = False)
    parser.add_argument('-m','--msisdn', help='The MSISDN you want to extract to',required = False)
    return parser.parse_args()

#A function to run the shell commands
def run_cmd(command):
    proc = s.Popen(command, shell=True, stdout=s.PIPE, stderr=s.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, str(s_output).replace("b'","").replace("\\n'",""), s_err

#A function to format the output and remove the unneeded charchters
def convertTuples(tup,delimiter):
    str_v = ''
    for item in tup:
        str_v += str(item)+delimiter
    return str_v.strip(delimiter)

#A function to check the input msisdn
def checkMSISDN(msisdn):
    try:
        if(msisdn != None):
            aPhoneNum = msisdn.split(" ")                                                                               # split the msisdn to do a list of them
            validPhoneNum = ['0', '1', '2', '3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9']                                  # the valid msisdns
            diffChecker = all(item in validPhoneNum for item in aPhoneNum)              # to check if the passed msisdns are valid
            for i in aPhoneNum:
                if(diffChecker):
                    return True
                else:
                    logging.error("Wrong msisdn parameter passing!\nIt should be in \"0 1 2 3 4 5 6 7 8 9\"")
                    exit()
        else:
            return False
    except Exception as e:
        logging.error("Passing the msisdn param failed due to: {0}".format(str(e)))
        exit()

#A function to compress the files
def compress(fileName,delimiter):
    with open(f"{fileName}", 'rb') as src, gzip.open(f"{fileName.replace('.csv','')}.gz", 'wb') as dst:
        dst.writelines(src)
        run_cmd("rm -r {0}".format(fileName))
    return dst

# The below four functions used to check the connection string validity
def checkConnStringOracle(connString):
    try:
        #check if the connection string has jdbc at the begining or not and do the needs if any
        if(connString.find('jdbc:oracle:thin:') == 0 and 'HOST' in connString):
            connStr = connString.split(":")[3:]
            return str(connStr)
        if (connString.find('jdbc:oracle:thin:') == -1  and 'HOST' in connString):
            connStr = connString
            return connStr
        #check if the connection string is wrong and exit
        else:
            logging.error("Check your connection string!")
            logging.error("It must be $userName/$password@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$IP)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=$DB or $service_name)))")
            exit()
    except Exception as e:
            logging.error("Connection failed due to {0}".format(str(e)))

def checkConnStringMySql(connString):
    try:
        #check if the connection string has jdbc at the begining or not and do the needs if any
        if(connString.find('jdbc:mysql://') == 0):
            connStr = connString.split("://")[1:]
            return str(connStr)
        if (connString.find('jdbc:mysql://') == -1):
            connStr = connString
            return connStr
        #check if the connection string is wrong and exit
        else:
            logging.error("Check your connection string!")
            logging.error("It must be userName:Password@hostName:Port/DB")
            exit()
    except Exception as e:
            logging.error("Check your connection string!")
            logging.error("It must be userName:Password@hostName:Port/DB")
            logging.error("Connection failed due to {0}".format(str(e)))

def checkConnStringSqlServer(connString):
    try:
        #check if the connection string has jdbc at the begining or not and do the needs if any
        if(connString.find('jdbc:sqlserver://') == 0 and 'user=' in connString ):
            connStr = connString.split("://")[1:]
            return str(connStr)
        if (connString.find('jdbc:sqlserver://') == -1 and 'user=' in connString ):
            connStr = connString
            return connStr
        #check if the connection string is wrong and exit
        else:
            logging.error("Check your connection string!")
            logging.error("It must be hostName:Port;databaseName=xxx;user=xxx;password=xxx OR hostName;databaseName=xxx;user=xxx;password=xxx")
            exit()
    except Exception as e:
            logging.error("Check your connection string!")
            logging.error("It must be hostName:Port;databaseName=xxx;user=xxx;password=xxx OR hostName;databaseName=xxx;user=xxx;password=xxx")
            logging.error("Connection failed due to {0}".format(str(e)))

def checkConnStringMongo(connString):
    try:
        #check if the connection string has jdbc at the begining or not and do the needs if any
        if(connString.find('mongodb://') == 0):
            connStr = connString.split("://")[1:]
            return str(connStr)
        if (connString.find('mongodb://') == -1):
            connStr = connString
            return connStr
        #check if the connection string is wrong and exit
        else:
            logging.error("Check your connection string!")
            logging.error("It must be mongodb://{user}:{password}@{host}:{port} OR {user}:{password}@{host}:{port}")
            exit()
    except Exception as e:
            logging.error("Check your connection string!")
            logging.error("It must be mongodb://{user}:{password}@{host}:{port} OR {user}:{password}@{host}:{port}")
            logging.error("Connection failed due to {0}".format(str(e)))

# The below four functions used to get the connection string params
def getConnCredentialsOracle(connString):
    try:
        connFunc=checkConnStringOracle(connString)
        hostName = connFunc[connFunc.find("HOST"):connFunc.find("PORT")-2].split("=")[1]                   #getting the host name
        serviceName = connFunc[connFunc.find("SERVICE_NAME"):connFunc.find(")))")].split("=")[1]           #getting the service name
        port =  connFunc[connFunc.find("PORT"):connFunc.find("CONNECT_DATA")-3].split("=")[1]              #getting the port
        userName = connFunc.split("/")[0].replace("[","").replace("'","")                                  #getting the userName
        password = connFunc.split("/")[1].split("@")[0]                                                    #getting the password
        return hostName, serviceName, port, userName, password
    except Exception as e:
            logging.error("Getting the connection string failed due to {0}".format(str(e)))
            exit()

def getConnCredentialsMySql(connString):
    try:
        connFunc=checkConnStringMySql(connString)
        hostName =  str(connFunc)[str(connFunc).rindex('@')+1:].split('/')[0].split(':')[0]                        #getting the host name
        port = str(connFunc)[str(connFunc).rindex('@')+1:].split('/')[0].split(':')[1]                             #getting the port
        serviceName = str(connFunc)[str(connFunc).rindex('@')+1:].split('/')[1].replace("\']","")                  #getting the service name
        userName = str(connFunc)[:str(connFunc).rindex('@')].split(':')[0].replace("[\'","")                       #getting the userName
        password = str(connFunc)[:str(connFunc).rindex('@')].split(':')[1]                                         #getting the password
        return hostName,serviceName, port, userName, password
    except Exception as e:
            logging.error("Getting the connection string failed due to {0}".format(str(e)))
            exit()

def getConnCredentialsSqlServer(connString):
    try:
        connFunc=checkConnStringSqlServer(connString)
        hostName =  str(connFunc).split(";")[0].replace("['","").split(":")[0]                 #getting the host name
        try:
            port = str(connFunc).split(";")[0].split(":")[1]                                       #getting the port
        except Exception as e:
            port = "0"
        serviceName = str(connFunc).split(";")[1].split("=")[1]                                #getting the service name
        userName = str(connFunc).split(";")[2].split("=")[1]                                   #getting the userName
        password =  str(connFunc).split(";")[3].split("=")[1]                                  #getting the password
        return hostName,serviceName, port, userName, password
    except Exception as e:
            logging.error("Getting the connection string failed due to {0}".format(str(e)))
            exit()

def getConnCredentialsMongo(connString):
    try:
        connFunc=checkConnStringMongo(connString)
        hostName =  str(connFunc)[str(connFunc).rindex('@')+1:].split('/')[0].split(':')[0]                     #getting the host name
        port = str(connFunc)[str(connFunc).rindex('@')+1:].split('/')[0].split(':')[1].replace("\']","")        #getting the port
        userName = str(connFunc)[:str(connFunc).rindex('@')].split(':')[0].replace("['","")                     #getting the userName
        password = str(connFunc)[:str(connFunc).rindex('@')].split(':')[1]                                      #getting the password
        serviceName = str(connString)[str(connString).rindex('@'):].split(':')[1].split('/')[1]                 #getting the service name
        return hostName, port, userName, password, serviceName
    except Exception as e:
            logging.error("Getting the connection string failed due to {0}".format(str(e)))
            exit()

#A function to format and split -if needed- the output files
def genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF):
    try:
        fileSeq = 1
        lineCounter,lineCounter2 = 0,0
        startTime = getSysTime()
        logging.basicConfig(level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
        run_cmd("mkdir -p {0}".format(f"{extraction_path}/{feedName}/{date_v}"))
        to_file = f"{date_v}_{feedName}_{fileSeq}.csv"
        path_to_file=f"{extraction_path}/{feedName}/{date_v}/{to_file}"
        lineCount = len(rows)
        f=open(path_to_file,'a')
        for row in rows:
            f.writelines(convertTuples(row,delimiter)+'\n')
            convertTuples(row,delimiter)
            lineCounter+=1
            lineCounter2+=1
            if (lineCounter2 == 10000):
                logging.info("{0} lines have been written to file: {1} so far".format(lineCounter,to_file.replace('.csv','.gz')))
                lineCounter2=0
            if(lineCounter >= recPF):
                logging.info("summary: file: {0},lines: {1}, stime:{2} etime:{3}".format(to_file.replace('.csv','.gz'),lineCounter,startTime,getSysTime()))
                f.close()
                compress(path_to_file,delimiter)
                fileSeq +=1
                to_file = f"{date_v}_{feedName}_{fileSeq}.csv"
                path_to_file=f"{extraction_path}/{feedName}/{date_v}/{to_file}"
                f=open(path_to_file,'a')
                lineCounter =0
        f.close()
        compress(path_to_file,delimiter)
        logging.info("{0} lines have been written to file: {1} so far".format(lineCounter,to_file))
        logging.info("summary: Extracted lines: {0}, stime:{1} etime:{2}".format(lineCount,startTime,getSysTime()))
    except Exception as e:
            logging.error("Generating an extraction file failed due to {0}".format(str(e)))
            exit()

#A function to format and split -if needed- the output Mongo files
def genericMongoFileWriter(connString,feedName,date_v,extraction_path,recPF,db,query=None):
    try:
        iArr=[]
        fileSeq = 1
        lineCounter,lineCounter2 = 0,0
        startTime = getSysTime()
        connCred = getConnCredentialsMongo(connString)
        logging.basicConfig(level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
        run_cmd("mkdir -p {0}".format(f"{extraction_path}/{feedName}/{date_v}"))
        to_file = f"{date_v}_{feedName}_{fileSeq}.json"
        path_to_file=f"{extraction_path}/{feedName}/{date_v}/"
        dict_count= db[feedName].count()
        logging.info("dict has {0} objects.. ".format(dict_count))
        # check if it a snapshot spool or conditioned one
        if(query == None):
             #getting the _ids into a list to loop on last digit
             logging.info("getting the _ids to work on..")
             ids_array_query=list(db[feedName].find({},'_id'))
             query="{\"_id\":{\"$regex\":\"{6}$\"}}"
             dfRows = pnd.DataFrame(ids_array_query)
             dectRows_q = dfRows.iloc[:,0].to_json(orient = 'records')#,lines=True)
             dectRows = dectRows_q.replace("\"","").replace("[","").replace("]","").split(',')
             #getting the last char from the _ids
             for i in dectRows:
                 iArr.append(i[-1:])
             #remove the duplicate _ids
             iArr = set(iArr)
             logging.info(f"running for {iArr} list of _ids.. ")
             #exit()
             # loop through _ids to split the query for the best performance
             for i in iArr:
                 logging.info(f"Running for {i} _id.. ")
                 command="mongoexport --host=\"{0}\" --port={1} --username {2} --password {3} --collection={4} --db={5} --query '{10}' --out={7}{8}_{9}_{6}.json".format(connCred[0],connCred[1],connCred[2],connCred[3],feedName,connCred[4],i,path_to_file,feedName,date_v,query.replace("{6}",str(i)))
                 run_cmd(command)
        else:
            command="mongoexport --host=\"{0}\" --port={1} --username {2} --password {3} --collection={4} --db={5} --query '{9}' --out={6}{7}_{8}.json".format(connCred[0],connCred[1],connCred[2],connCred[3],feedName,connCred[4],path_to_file,feedName,date_v,query)
            logging.info(f"command to use\n{command}")
            run_cmd(command)
    except Exception as e:
            logging.error("Generating an extraction file failed due to {0}".format(str(e)))
            exit()


#A function to format and split -if needed- the output files (with MSISDN)
def genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,msisdn):
    try:
        fileSeq = 1
        lineCounter,lineCounter2 = 0,0
        startTime = getSysTime()
        logging.basicConfig(level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
        run_cmd("mkdir -p {0}".format(f"{extraction_path}/{feedName}/{date_v}"))
        to_file = f"{date_v}_{feedName}_{fileSeq}_{msisdn}.csv"
        path_to_file=f"{extraction_path}/{feedName}/{date_v}/{to_file}"
        lineCount = len(rows)
        f=open(path_to_file,'a')
        for row in rows:
            f.writelines(convertTuples(row,delimiter)+'\n')
            convertTuples(row,delimiter)
            lineCounter+=1
            lineCounter2+=1
            if (lineCounter2 == 10000):
                logging.info("{0} lines have been written to file: {1} so far".format(lineCounter,to_file.replace('.csv','.gz')))
                lineCounter2=0
            if(lineCounter >= recPF):
                logging.info("summary: file: {0},lines: {1}, stime:{2} etime:{3}".format(to_file.replace('.csv','.gz'),lineCounter,startTime,getSysTime()))
                f.close()
                compress(path_to_file,delimiter)
                fileSeq +=1
                to_file = f"{date_v}_{feedName}_{fileSeq}_{msisdn}.csv"
                path_to_file=f"{extraction_path}/{feedName}/{date_v}/{to_file}"
                f=open(path_to_file,'a')
                lineCounter =0
        f.close()
        compress(path_to_file,delimiter)
        logging.info("{0} lines have been written to file: {1} so far".format(lineCounter,to_file))
        logging.info("summary: Extracted lines: {0}, stime:{1} etime:{2}".format(lineCount,startTime,getSysTime()))
    except Exception as e:
            logging.error("Generating an extraction file failed due to {0}".format(str(e)))
            exit()

# The main function: used to connect to the needed DB and fetch the results
def connect(feedName,connString,selectedQuery,snapshot,date_v,start_hr,end_hr,lastDigit,database_type,extraction_path,delimiter,recPF):
    try:
        logging.basicConfig(level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
        if(database_type == "Oracle"):
            queryPrinter=selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)
            connCred = getConnCredentialsOracle(connString)
            dsn_tns = cx_Oracle.makedsn(connCred[0], connCred[2], service_name=connCred[1])            # make the dns to establish an Oracle connection
            conn = cx_Oracle.connect(user=rf'{connCred[3]}', password=rf'{connCred[4]}', dsn=dsn_tns)  #connect to Oracle DB \ if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
            c = conn.cursor()
            logging.info("Running the extraction query: {0}".format(queryPrinter))
            if(snapshot=="true"):
                logging.info("Snapshot table, Running the query without a date..")
                if(lastDigit!="11"):
                    c.execute(selectedQuery.format(strHr=start_hr,endHr=end_hr,msisdn=lastDigit)) # excute the query with date
                    rows= c.fetchall()
                    genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,lastDigit)   #write the records into files
                else:
                    c.execute(selectedQuery.format(strHr=start_hr,endHr=end_hr)) # excute the query with date
                    rows= c.fetchall()                                                        # fetch the query res
                    genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF)   #write the records into files

            else:
                if(lastDigit!="11"):
                    c.execute(selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)) # excute the query with date
                    rows= c.fetchall()
                    genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,lastDigit)   #write the records into files
                else:
                    c.execute(selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr)) # excute the query with date
                    rows= c.fetchall()                                                        # fetch the query res
                    genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF)   #write the records into files

            conn.close()            # close the connection
        elif(database_type == "SqlServer"):
            queryPrinter=selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)
            # for the SqlServer connection string
            connCredSql = getConnCredentialsSqlServer(connString)
            if(connCredSql[2] == "0"):
                connSql = pd.connect('Driver={ODBC Driver 13 for SQL Server};'                                  #Connect to SQL Server DB
                                     f'Server={connCredSql[0]};'
                                     f'Database={connCredSql[1]};'
                                     f'UID={connCredSql[3]};PWD={connCredSql[4]}'
                                     )

            else:
                connSql = pd.connect('Driver={ODBC Driver 13 for SQL Server};'                                  #Connect to SQL Server DB
                                     f'Server={connCredSql[0]},{connCredSql[2]};'
                                     f'Database={connCredSql[1]};'
                                     f'UID={connCredSql[3]};PWD={connCredSql[4]}'
                                     )
            c = connSql.cursor()
            logging.info("Running the extraction query: {0}".format(queryPrinter))
            if(snapshot=="true"):
                logging.info("Snapshot table, Running the query without a date..")
                if(lastDigit!="11"):
                    c.execute(selectedQuery.format(strHr=start_hr,endHr=end_hr,msisdn=lastDigit)) # excute the query with date
                    rows= c.fetchall()
                    genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,lastDigit)   #write the records into files
                else:
                    c.execute(selectedQuery.format(strHr=start_hr,endHr=end_hr)) # excute the query with date
                    rows= c.fetchall()                                                        # fetch the query res
                    genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF)   #write the records into files
            else:
                if(lastDigit!="11"):
                    c.execute(selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)) # excute the query with date
                    rows= c.fetchall()
                    genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,lastDigit)   #write the records into files
                else:
                    c.execute(selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr)) # excute the query with date
                    rows= c.fetchall()                                                           # fetch the query res
                    genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF)   #write the records into files

            connSql.close()                                                                  # close the connection

        elif(database_type == "MongoDB"):
            # for the MongoDB connection string
            connCred = getConnCredentialsMongo(connString)
            logging.info(f"INFO: connecting using the connection string.. mongodb://{connCred[2]}:{connCred[3]}@{connCred[0]}:{connCred[1]}/{connCred[4]}")
            client = MongoClient(f"mongodb://{connCred[2]}:" + urllib.parse.quote(f"{connCred[3]}") + f"@{connCred[0]}:{connCred[1]}/?authSource="f"{connCred[4]}")
            db = client[connCred[4]]
            logging.info("You are connected!")
            if (len(selectedQuery.strip()) == 0):
                logging.info("INFO: full snapshot {0} collection!".format(feedName))
                logging.info("Running the extraction query: {0}".format(f"db.{feedName}.find()"))
                genericMongoFileWriter(connString,feedName,date_v,extraction_path,recPF,db)
            else:
                logging.info("Running the extraction query: {0}".format(f"db.{feedName}.find({selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)})"))
                genericMongoFileWriter(connString,feedName,date_v,extraction_path,recPF,db,selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr))
            client.close()            # close the connection
        else:
            queryPrinter=selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)
            connCredSql = getConnCredentialsMySql(connString)
            connSql = Myconn.connect(user=connCredSql[3],password=connCredSql[4],database=connCredSql[1],host=connCredSql[0],port=connCredSql[2])      # Connect to MySQL DB
            db_Info  = connSql.get_server_info()                                                                                                        # Get the serverInfo
            logging.info(f"Connected to MySQL Server version: {db_Info}")
            c = connSql.cursor()
            logging.info("Running the extraction query: {0}".format(queryPrinter))
            if(snapshot=="true"):
                logging.info("Snapshot table, Running the query without a date..")
                if(lastDigit!="11"):
                    c.execute(selectedQuery.format(strHr=start_hr,endHr=end_hr,msisdn=lastDigit)) # excute the query with date
                    rows= c.fetchall()
                    genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,lastDigit)   #write the records into files
                else:
                    c.execute(selectedQuery.format(strHr=start_hr,endHr=end_hr)) # excute the query with date
                    rows= c.fetchall()                                                        # fetch the query res
                    genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF)   #write the records into files
            else:
                if(lastDigit!="11"):
                    c.execute(selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr,msisdn=lastDigit)) # excute the query with date
                    rows= c.fetchall()
                    genericFileWriterWithMSISDN(feedName,rows,date_v,extraction_path,delimiter,recPF,lastDigit)   #write the records into files
                else:
                    c.execute(selectedQuery.format(DATE=date_v,strHr=start_hr,endHr=end_hr)) # excute the query with date
                    rows= c.fetchall()                                                           # fetch the query res
                    genericFileWriter(feedName,rows,date_v,extraction_path,delimiter,recPF)   #write the records into files

            connSql.close()                                                                  # close the connection
    except Exception as e:
            logging.error("Connection failed due to {0}".format(str(e)))
            exit()

def main() :
    #get args
    args=getArgs()
    configData = readJsonFile(args.configFile)
    date_v = args.extractionDate                # get the extraction date passed by the user
    start_hr = args.startHour                   # get the extraction start hour passed by the user
    end_hr = args.endHour                       # get the extraction end hour passed by the user
    msisdn = args.msisdn                        # get the extraction msisdn passed by the user
    feeds = configData["Feeds"]                 # get the feeds from the config file
    paths = configData["paths"]                 # get the needed paths from the config file
    connections = configData["Connections"]
    toolLogPath = "{0}/{1}".format(paths["logPath"],getServerDate())
    toolLogFile = "{0}/{1}_{2}.log".format(toolLogPath,args.feedName,getTime())
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
    feedList =[]

    if(str(configData).find("recPerFile")== -1):                #check the number of rec per file and the default to 200K/file
        recPFile = 200000
    else:
        if(configData["recPerFile"]<10000):                     #check the min #of rec/file and make it 20K
            log.warn("The number of records per file should be minimum 20000 at recPerFile param!")
            recPFile = 20000
        else:
            recPFile=configData["recPerFile"]

    for feed in feeds:                                           #add the config file feeds to a list
        feedList.append(feed["feed_name"])

    if(args.feedName not in feedList):                          #check if the feed is in the previous created list
        log.error("{0} feed doesn't exist at the config file!".format(args.feedName))
        exit()

    for i in range(len(feeds)):
        if(feeds[i]["feed_name"] == args.feedName):
            if(feeds[i]["conn"] in connections):      # get connection string if it passed and if not show an Error!
                connString=connections[feeds[i]["conn"]]
            else:
                log.error("Connection string doesn't exist at the config file!")
                exit()

            feedQuery = feeds[i]["query"]             # get the extraction query
            feedName = feeds[i]["feed_name"]
            extraction_path = paths["extractionPath"]

            if( "delimiter" not in feeds[i] or feeds[i]["delimiter"]==""):            # get delimiter if it passed and if not get the defualt value
                dDelimiter = configData["dDelimiter"]
            else:
                dDelimiter = feeds[i]["delimiter"]
            if("incoming_dir" not in feeds[i] or feeds[i]["incoming_dir"]==""):         # get incoming dir if it passed and if not get the defualt value
                inDir = paths["incomingPath"]
            else:
                inDir=feeds[i]["incoming_dir"]

            if("snapshot" not in feeds[i] or feeds[i]["snapshot"]=="" or feeds[i]["snapshot"]=="false"):             # get if the feed is a snapshot or not
                snapshot = "false"
            else:
                snapshot = feeds[i]["snapshot"]

            if("DB" not in feeds[i] or feeds[i]["DB"]=="" or feeds[i]["DB"].lower() == "oracle"):                       # get the DB type
                database = "Oracle"
            else:
                if(feeds[i]["DB"].lower() == "mysql"):
                    database = "MySQL"
                elif(feeds[i]["DB"].lower() == "sqlserver"):
                    database = "SqlServer"
                elif(feeds[i]["DB"].lower() == "mongodb"):
                    database = "MongoDB"
                else:
                    log.error("DataBase type isn't supported!\nPlease choose between\n1-MySQL\n2-Oracle\n3-SqlServer\n4-MongoDB")
                    exit()

            if("hdfs" not in feeds[i] or feeds[i]["hdfs"]==""):                                                        # get the hdfs path
                hdfsPathTmp = configData["hdfsPath"]
                hdfsPath = f"{hdfsPathTmp}/{feedName}/"
            else:
                hdfsPath = feeds[i]["hdfs"]

            path_to_file=f"{extraction_path}/{feedName}/{date_v}"
            run_cmd("rm -r {0}".format(path_to_file))
            log.info("Removing the hdfs data {0} if exists".format(f"{hdfsPath}tbl_dt={date_v}/*"))
            run_cmd("hdfs dfs -rm {0}".format(f"{hdfsPath}tbl_dt={date_v}/*"))
            log.info("Removing the extraction tmp directory {0} if exists".format(path_to_file))
            run_cmd("rm -r {0}".format(f"{inDir}/{date_v}"))
            log.info("Removing the incoming directory {0} if exists".format(f"{inDir}/{date_v}"))
            if(checkMSISDN and msisdn != None):                                                       #checking if the user passed an msisdn to extract
                msisdnSplit = msisdn.split(" ")
                for lastDigit in msisdnSplit:
                    connect(feedName,connString,feedQuery,snapshot,date_v,start_hr,end_hr,lastDigit,database,extraction_path,dDelimiter,recPFile)
            else:
                connect(feedName,connString,feedQuery,snapshot,date_v,start_hr,end_hr,"11",database,extraction_path,dDelimiter,recPFile)
            log.info("Dropping the extracted data into tmp directory {0}".format(path_to_file))
            run_cmd("mv {0} {1}".format(path_to_file,inDir))
            log.info("Moving the data from {0} to {1}".format(path_to_file,inDir))
            log.info("The job finished at {0}".format(getTime()))


if __name__ == "__main__" :
    main()