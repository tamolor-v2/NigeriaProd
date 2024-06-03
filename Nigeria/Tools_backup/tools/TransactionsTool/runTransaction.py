import os
import sys
import argparse
import subprocess as s
import datetime 
import calendar
import json
import configparser

import time

from datetime import datetime, timedelta
from Postgres import postgres
from Presto import presto
import logging


def getServerDateTime():
    return (datetime.today()).strftime('%Y%m%d_%H%M%S')

def getServerDate():
    return (datetime.today()).strftime('%Y%m%d')

def readJsonFile(path):
    data = json.loads(open(path).read())
    return data

def getArgs():
    parser = argparse.ArgumentParser(prog='parsePlotSens')
    parser.add_argument('-cf','--configPath', help='configPath',required = True)
    parser.add_argument('-sd','--startDate', help='startDate',nargs='?', const='',required = False)
    parser.add_argument('-ed','--endDate', help='endDate',nargs='?', const='',required = False)
    parser.add_argument('-rd','--runDate', help='runDate',nargs='?', const='',required = False)
    parser.add_argument('-qn','--queryName', help='queryName',required = False)
    parser.add_argument('-pc','--prestoCatalog', help='prestoCatalog',nargs='?', const='',required = False)
    parser.add_argument('-et','--executType', help='executType',nargs='?', const='',required = False)
    parser.add_argument('-s','--simulation', help='simulation',nargs='?', const='',required = False)
    parser.add_argument('-st','--shiftTime', help='shiftTime',nargs='?', const='',required = False)
    # parser.add_argument('-e','--email', help='email',nargs='?', const='',required = False)
    return parser.parse_args()

def readTemplate(path):
    with open(path, 'r') as file:
        data = file.read().replace('\n', ' ')
        return data



def updateQueryTemplate(query,dictionary,startDate,endDate,shiftTime=0):
    #all replace will active on startDate
    #Warning Note: if you use word in dictionary this will replace it depending on it
    #we can use startDate as runDate this mean run for one day
    #get year, month and day from startDate 
    year=int(startDate[0:4])
    month=int(startDate[4:6])
    day=int(startDate[6:8])

    eyear=int(endDate[0:4])
    emonth=int(endDate[4:6])
    eday=int(endDate[6:8])
    shiftTime = shiftTime
    staticDateAndTime=(datetime.today()).strftime('%Y%m%d%H%M%S')

    #first day of Month from startDate
    firstDayOfMonth=(datetime(year=year, month=month, day=1)).strftime('%Y%m%d')

    #same firstDayOfMonth
    monthAgo1tmp=firstDayOfMonth

    #last day of Month from startDate
    lastDayOfLastMonth=(datetime(year=int(firstDayOfMonth[0:4]), month=int(firstDayOfMonth[4:6]), day=int(firstDayOfMonth[6:8]) ) - timedelta(days=1)).strftime('%Y%m%d')

    #day -1 from startDate
    ydate=(datetime(year=year, month=month, day=day) - timedelta(days=1)).strftime('%Y%m%d')

    yyyymmddRunDate=startDate

    #day +1 from startDate
    startDatePlusOne=(datetime(year=year, month=month, day=day) + timedelta(days=1)).strftime('%Y%m%d')
  
    #StartDatePlusaWeek
    yyyyRunDateWeek=(datetime(year=year, month=month, day=day) + timedelta(days=6)).strftime('%Y%m%d')
    yyyyRunDateWee=(datetime(year=year, month=month, day=day) + timedelta(days=5)).strftime('%Y%m%d')
 
    #StartDatePlus3
    runDatePlus3=(datetime(year=year, month=month, day=day) + timedelta(days=3)).strftime('%Y%m%d')

    #this same  startDatePlusOne
    tdate=startDatePlusOne

    #day -90 from startDate
    y90day=(datetime(year=year, month=month, day=day) - timedelta(days=90)).strftime('%Y%m%d')

    #day -30 from startDate
    yyyyRunDateMonth=(datetime(year=year, month=month, day=day) - timedelta(days=30)).strftime('%Y%m%d')
    
    #day -29 from startDate
    y30day=(datetime(year=year, month=month, day=day) - timedelta(days=29)).strftime('%Y%m%d')

    #day -19 from startDate
    y20day=(datetime(year=year, month=month, day=day) - timedelta(days=19)).strftime('%Y%m%d')

    #day -20 from startDate
    y21day=(datetime(year=year, month=month, day=day) - timedelta(days=20)).strftime('%Y%m%d')

    #day -9 from startDate
    y10day=(datetime(year=year, month=month, day=day) - timedelta(days=9)).strftime('%Y%m%d')

    #day -10 from startDate
    y11day=(datetime(year=year, month=month, day=day) - timedelta(days=10)).strftime('%Y%m%d')

    #day -5 from startDate
    y6day=(datetime(year=year, month=month, day=day) - timedelta(days=5)).strftime('%Y%m%d')

    #get month by format "yyyymm" from day-1 from startDate
    vmonth=(datetime(year=year, month=month, day=day) + timedelta(days=1)).strftime('%Y%m')

    #get day by format "dd" from day-1 from startDate
    vday=(datetime(year=year, month=month, day=day) + timedelta(days=1)).strftime('%d')

    #get month from startDate
    monthAgo=(datetime(year=year, month=month, day=day)).strftime('%Y%m')

    #get last month by format "yyyymm" from startDay month 
    y1month=(datetime(year=int(firstDayOfMonth[0:4]), month=int(firstDayOfMonth[4:6]), day=int(firstDayOfMonth[6:8]) ) - timedelta(days=1)).strftime('%Y%m')

    #get last two month by format "yyyymm" from startDay month
    y2month=(datetime(year=int(y1month[0:4]), month=int(y1month[4:6]), day=1 ) - timedelta(days=1)).strftime('%Y%m')

    #get format "yyyy-mm-dd" from startDate wich format like "yyyymmdd"
    sDashedFormat=(datetime(year=year, month=month, day=day)).strftime('%Y-%m-%d')

    #get format "yyyy-mm-dd" from endDate wich format like "yyyymmdd"
    eDashedFormat=(datetime(year=eyear, month=emonth, day=eday)).strftime('%Y-%m-%d')

    startDateYYYYMMDDFromat=(datetime(year=year, month=month, day=day)).strftime('%Y%m%d')

    endDateYYYYMMDDFromat=(datetime(year=eyear, month=emonth, day=eday)).strftime('%Y%m%d')

    query=str(query)

    #replace shift Time from epoch

    for key in dictionary:
        if(key["name"] in locals()):
            logging.info("name:{0} , value:{1} , desc:{2}".format(key["name"],locals()[key["name"]],key["description"]))
            query=query.replace(key["name"],str(locals()[key["name"]]))
    return query



def executePostgresTransaction(query,postgresHost,postgresPort,postgresDatabase,postgresUsername,postgresPassword):
    oPostgres=postgres(postgresHost,postgresPort,postgresDatabase,postgresUsername,postgresPassword)
    oPostgres.executeTransaction(query)

def executePrestoTransaction(query,prestoHost,prestoPort,prestoCatalog,prestoUsername,prestoPassword,prestoSSL,prestoSSLKeyStorePassword,prestoSSLKeyStorePath,prestojarPath,logger_name):
    oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username=prestoUsername,password=prestoPassword,SSL=prestoSSL,SSLKeyStorePassword=prestoSSLKeyStorePassword,SSLKeyStorePath=prestoSSLKeyStorePath,jarPath=prestojarPath,logger_name=logger_name)
    oPresto.executeTransaction(query)


#run shell command
def run_cmd(command):
    proc = s.Popen(command, shell=True, stdout=s.PIPE, stderr=s.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, str(s_output).replace("b'","").replace("\\n'",""), s_err


if __name__ == '__main__':
    #get args
    args=getArgs()
    
    configData = readJsonFile(args.configPath)

    toolLogPath = "{0}/{1}".format(configData["paths"]["toolLog"],getServerDate())
    toolLogFile = "{0}/{1}_{2}.log".format(toolLogPath,args.queryName,getServerDateTime())

    #make dir log path
    run_cmd("mkdir -p {0}".format(toolLogPath))

    #config the log
    level=logging.INFO
    FORMAT = '%(levelname)s %(asctime)s %(name)s - %(message)s'
    formatter = logging.Formatter(FORMAT)

    #init logger
    logger_name="runTransaction_{0}".format(args.queryName)
    log = logging.getLogger(logger_name)
    log.setLevel(level)

    #logger to writing on file \ you can ignore if you don't want to writ log file
    fh = logging.FileHandler(toolLogFile, mode='w', encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    #logger to print into screen \ you can ignore if you don't want to print on screen
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    if(args.startDate == None and args.endDate == None):
        startDate = args.runDate
        endDate= args.runDate
    else:
        startDate = args.startDate
        endDate= args.endDate
        
    

    #get postgres config
    postgresHost=configData["connection"]["postgres"]["host"]
    postgresPort=configData["connection"]["postgres"]["port"]
    postgresDatabase=configData["connection"]["postgres"]["database"]
    postgresUsername=configData["connection"]["postgres"]["username"]
    postgresPassword=configData["connection"]["postgres"]["password"]    

    #get preto config
    prestoHost=configData["connection"]["presto"]["host"]
    prestoPort=configData["connection"]["presto"]["port"]
    prestoCatalog=configData["connection"]["presto"]["catalog"]
    if(args.prestoCatalog != None):
        if(args.prestoCatalog):
            prestoCatalog=args.prestoCatalog
        else:
            prestoCatalog=configData["connection"]["presto"]["catalog"]
    prestoUsername=configData["connection"]["presto"]["username"]
    prestoPassword=configData["connection"]["presto"]["password"]    
    prestoSSL=configData["connection"]["presto"]["SSL"]
    prestoSSLKeyStorePath=configData["connection"]["presto"]["SSLKeyStorePath"]
    prestoSSLKeyStorePassword=configData["connection"]["presto"]["SSLKeyStorePassword"]
    prestojarPath=configData["connection"]["presto"]["jarPath"]


    #get path of html template
    toolTmpPath=configData["paths"]["toolTmp"]

    #get dictionary
    dictionary=configData["dictionary"]

    #get query by index
    queries = configData["queries"]
    for query in queries:
        if(query["queryName"] == args.queryName):
            queryTemplate = readTemplate(query["queryPath"])
            executType=query["executType"]

    log.info("Selected Template Query:{0}".format(args.queryName))

    #take shift time from 
    shiftTime = 0
    if(args.shiftTime != None):
        shiftTime = int(args.shiftTime)

    queryTemplate = updateQueryTemplate(queryTemplate,dictionary,startDate,endDate,shiftTime=shiftTime)

    log.info("Updated Template Query:{0}".format(queryTemplate))

    if(args.simulation == None):
        if(args.executType !=None):
            executType=str(args.executType).upper()
        if(executType == "PRESTO"):
            start_time = time.time()
            log.warning("START TRANSACTION...")
            try:
                executePrestoTransaction(queryTemplate,prestoHost,prestoPort,prestoCatalog,prestoUsername,prestoPassword,prestoSSL,prestoSSLKeyStorePassword,prestoSSLKeyStorePath,prestojarPath,logger_name)
            except ValueError as v:
                log.error(v)
                log.warning("END TRANSACTION")
                raise v                
            log.warning("END TRANSACTION")
            log.warning("TRANSACTION TIME: {0}".format(time.time() - start_time))
        elif(executType == "POSTGRES"):
            start_time = time.time()
            log.warning("START TRANSACTION...")
            try:
                executePostgresTransaction(queryTemplate,postgresHost,postgresPort,postgresDatabase,postgresUsername,postgresPassword)
            except ValueError as v:
                log.error(v)
                log.warning("END TRANSACTION")
                raise v
            log.warning("END TRANSACTION")
            log.warning("TRANSACTION TIME: {0}".format(time.time() - start_time))
        else:
            log.error("execution type if presto or postgres")
    else:
        log.info("SIMULATION TRANSACTION OUTPUT: {0}".format(queryTemplate))
        print(queryTemplate.split(";"))
