from datetime import date,datetime, timedelta
import os,sys,fileinput
import subprocess as s
import time
import argparse
import csv
import logging as log
import pandas as pnd
import numpy  as np



## A function to get the 1st day of prev month - to use at the files status email -
def getDates():
    last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
    firstOfPrevMonth=str(date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)).replace('-','')
    return firstOfPrevMonth

## A function to get the 1st day of current month - to use at the files status email -
def getCurrDates():
    import datetime 
    todayDate = datetime.date.today()
    firstOfMon = str(datetime.date(todayDate.year, todayDate.month, 1)).replace('-','')
    return firstOfMon

queriesDictSLA={
"CELL_QOS_2G" : "select * from (select source,feed_name,date_key,date_diff('day',date_key,current_date) as days_late from stage.fb_CELL_QOS_2G_dq_rpt where file_received='N') x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and days_late>=escalation_days;",
"CELL_QOS_3G" : "select * from (select source,feed_name,date_key,date_diff('day',date_key,current_date) as days_late from stage.fb_CELL_QOS_3G_dq_rpt where file_received='N') x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and days_late>=escalation_days;",
"CELL_QOS_4G" : " select * from (select source,feed_name,date_key,date_diff('day',date_key,current_date) as days_late from stage.fb_CELL_QOS_4G_dq_rpt where file_received='N') x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and days_late>=escalation_days;",
}

queriesDictReceive={
"CELL_QOS_2G" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_percentage as DQ_Percent from stage.fb_CELL_QOS_2G_dq_rpt where file_received='Y' and report_month >={0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"CELL_QOS_3G" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_percentage as DQ_Percent from stage.fb_CELL_QOS_3G_dq_rpt where file_received='Y' and report_month >={0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"CELL_QOS_4G" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_percentage as DQ_Percent from stage.fb_CELL_QOS_4G_dq_rpt where file_received='Y' ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"CELL_INFO_2G" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='CELL_INFO_2G' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()), 
"CELL_INFO_3G" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='CELL_INFO_3G' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"CELL_INFO_4G" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='CELL_INFO_4G' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MTN_NG_GATEWAY_INFORMATION" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MTN_NG_GATEWAY_INFORMATION' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MAPS_SITE_INFO" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MAPS_SITE_INFO' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MTN_NG_3G_MONTHLY" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MTN_NG_3G_MONTHLY' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MTN_NG_4G_MONTHLY" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MTN_NG_4G_MONTHLY' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"TOPOLOGY_MAP" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='TOPOLOGY_MAP' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MTNN_ASSET_EXTRACT_2G_CELLS" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MTNN_ASSET_EXTRACT_2G_CELLS' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MTNN_ASSET_EXTRACT_3G_CELLS" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MTNN_ASSET_EXTRACT_3G_CELLS' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"MTNN_ASSET_EXTRACT_4G_CELLS" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='MTNN_ASSET_EXTRACT_4G_CELLS' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates()),
"POPULATION_DS" : "select * from (select report_month as Report_Month,source,feed_name as Feed_Name,date_key as Partition_Date,dq_index as DQ_Percent from stage.fb_feed_sla_dq_monthly  where feed_name='POPULATION_DS' and received_files>0 and report_month >= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name and escalation_days<1 order by Partition_Date;".format(getDates())
}

queriesDictValidation = {
"CELL_INFO_REPORT" : "select * from (select 'CELL INFO REPORT' as feed_name, sum(total_records) as total_records, sum(valid_record) as valid_records, sum(total_records)-sum(valid_record) as invalid_records, 1.000*sum(valid_record)/sum(total_records) as dq_precentage from stage.fb_cell_info_validation where report_month= {0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name".format(getCurrDates()),
"CELL_QOS_REPORT" : "select * from (select 'CELL QOS REPORT' as feed_name, sum(total_records) as total_records, sum(valid_record) as valid_records, sum(total_records)-sum(valid_record) as invalid_records, 1.000*sum(valid_record)/sum(total_records) as dq_precentage from stage.fb_cell_qos_validation where report_month={0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name".format(getCurrDates()),
"SITE_ENGAGEMENT_REPORT" : "select * from (select 'SITE ENGAGEMENT REPORT' as feed_name, sum(total_records) as total_records, sum(valid_record) as valid_records, sum(total_records)-sum(valid_record) as invalid_records, 1.000*sum(valid_record)/sum(total_records) as dq_precentage from stage.fb_site_engagement_validation where report_month={0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name".format(getCurrDates()),
"SITE_INFO_REPORT" : "select * from (select 'SITE INFO REPORT' as feed_name, sum(total_records) as total_records, sum(valid_record) as valid_records, sum(total_records)-sum(valid_record) as invalid_records, 1.000*sum(valid_record)/sum(total_records) as dq_precentage from stage.fb_site_info_validation where report_month={0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name".format(getCurrDates()),
"SYSTEM_ENGAGEMENT_REPORT" : "select * from (select 'SYSTEM ENGAGEMENT REPORT' as feed_name, sum(total_records) as total_records from stage.fb_system_engagement_validation where report_month={0} ) x left join (select feed_name,Contact_Name,Contact_Email,Contact_Mobile,Escalation_Level,Escalation_Days from stage.fb_email_escalation) y on x.feed_name=y.feed_name".format(getCurrDates())
}

def getArgs():
    parser = argparse.ArgumentParser(prog='parsePlotSens')
    parser.add_argument('-e','--emailFile', help='The sending email script file',required = True)
    parser.add_argument('-o','--outputFile', help='outputFile the output file for the queries',required = True)
    parser.add_argument('-t','--eType', help='Type of the email',required = True)
    parser.add_argument('-f','--feed', help='The feed name',required = False)
    return parser.parse_args()

def coalesce(*values):
    # """Return the first non-None value or None if all values are None"""
    return next((v for v in values if v != ''), 0)

## A function to run the shell cmds
def run_cmd(command):
    proc = s.Popen(command, shell=True, stdout=s.PIPE, stderr=s.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, str(s_output).replace("b'","").replace("\\n'",""), s_err

## A function to check if the output file exists to remove it before running
def checkExist(fileName):
    if(os.path.isfile(fileName)):
        log.info("Output file exists, removing the file..")
        run_cmd("rm {0}".format(fileName))
    else:
        log.info("Creating the output file..")
        run_cmd("touch {0}".format(fileName))

## A fun to check if the output file in csv format
def checkCSV(fileName):
    if(fileName.split('.')[1] != "csv"):
        log.error("The output file should be a csv formatted, Please pass a csv file!")
        exit ()

## A function to run the queries 
def runQuery(queriesDict,fileName):
     checkExist(fileName)
     checkCSV(fileName)
     args=getArgs()
     feedName=args.feed
     emailType=args.eType
     ## checking if the --feed arg has passed
     if(not feedName or emailType.upper() != "VALIDATION_ONE"):
         ## looping on the dict queries
         for feed in queriesDict:
             log.info("running the query for {0}..".format(feed))
             run_cmd("presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute \"{0}\" >> {1}".format(queriesDict[feed],fileName))
     else:
         ## checking if the passed feed is one of our list
         if(feedName in queriesDict):
             log.info("running the query for {0}..".format(feedName))
             run_cmd("presto --server 10.1.197.145:8999 --catalog hive5 --output-format CSV_HEADER --schema flare_8 --execute \"{0}\" >> {1}".format(queriesDict[feedName],fileName))
         else:
             log.error("The feed you passed {0} doesn't exist at the feeds list!".format(feedName))
             exit()

## A function to send the email and it details depends on the emailType passed
def sendFilesStatusEmail(scriptPath,fileName,emailType,feedName):
    ## reading the extarcted file 
    file = pnd.read_csv(fileName)
    dfFilename = pnd.DataFrame(file)
    res = dfFilename.iloc[:,[0,2,3,4]]
    ## getting the ppl to send an email to
    receveirsList=list(dict.fromkeys(list(dfFilename.iloc[:,7])))
    ## working on a tmp file to send via email then delete it
    newSendFile="/tmp/FB_Files_Status_{0}.csv".format(feedName)
    np.savetxt(r'{0}'.format(newSendFile), res.values, fmt='%s',delimiter=",",header="Report Month,Feed Name,Partition Date,DQ Percent",comments='') 
    run_cmd("scp {0} edge01002:/tmp".format(newSendFile))
    ## looping on these ppl
    for ppl in receveirsList:
        log.info("Sending an email to {0}".format(ppl))
        run_cmd("bash {0} \"{1}\" {2} {3}".format(scriptPath,feedName,newSendFile,"m.nabeel@ligadata.com"))#ppl))
    run_cmd("rm {0}".format(newSendFile))


## A function to send the email and it details depends on the emailType passed   
def sendEmail(scriptPath,fileName,emailType): 
    ## open the csv output file to work on -getting the emails and the needed info to send-
    with open(fileName, 'r') as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            if("feed" not in str(row)):
                log.info("Sending an email.. ")
                if(emailType == "SLA"):
                    if(row[3] !=0): ## check if the delayed days are more than 0 days to send an email
                        run_cmd("bash {0} \"{1}\" {2} {3} {4}".format(scriptPath,row[1],row[3],row[2],"m.nabeel@ligadata.com"))#,row[7]))
                elif(emailType == "VALIDATION_ONE"):
                    run_cmd("bash {0} \"{1}\" {2} {3} {4} ".format(scriptPath,row[0],row[1],getCurrDates(),"m.nabeel@ligadata.com"))#,row[4]))
                elif(emailType == "VALIDATION"):
                    run_cmd("bash {0} \"{1}\" {2} {3} {4} {5} {6} {7} ".format(scriptPath,row[0],row[1],row[2],row[3],row[4],getCurrDates(),"m.nabeel@ligadata.com"))#,row[7]))
                else:
                    log.error("Wrong passing args!")

                    

def main() :
    #config the log
    level=log.INFO
    FORMAT = '%(levelname)s %(asctime)s - %(message)s'
    formatter = log.Formatter(FORMAT)

    #init logger
    logger_name=""
    logg = log.getLogger(logger_name)
    logg.setLevel(level)

    #logger to print into screen
    ch = log.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logg.addHandler(ch)

    #getting the in and out files
    args=getArgs()
    fileName=args.outputFile
    scriptPath=args.emailFile
    emailType=args.eType
    feedName=args.feed
    ##checking the type of the emails - SLA or Files report - 
    if(emailType.upper() == "SLA"):
        runQuery(queriesDictSLA,fileName)
        sendEmail(scriptPath,fileName,emailType.upper()) 
    elif (emailType.upper() == "RECEIVED"):
        runQuery(queriesDictReceive,fileName)
        sendFilesStatusEmail(scriptPath,fileName,emailType.upper(),feedName)
    elif(emailType.upper() == "VALIDATION_ONE" or emailType.upper() == "VALIDATION"):
        runQuery(queriesDictValidation,fileName)
        sendEmail(scriptPath,fileName,emailType.upper())
    else:
        log.error("The email type must be on of:\n1.SLA\n2.RECEIVED\n3.VALIDATION_ONE\n4.VALIDATION")
        exit ()

if __name__ == "__main__" :
    main()

