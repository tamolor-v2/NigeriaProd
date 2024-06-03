##/nas/share05/ops/mtnops/Flytxt_Pre_Qualified_v2.py

# /*CREATE TABLE hive5.nigeria.flytxt_pqualified_msisdn (


#! /usr/bin/env /usr/bin/python3.6
import sys
import getopt
import os
import subprocess
import re
import ip_config as cfg
import cx_Oracle

from datetime import datetime
from datetime import timedelta
from pyhive import presto

##from prestodb
import time

import logging
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import subprocess

requests.packages.urllib3.disable_warnings()

# 10.1.197.145:8999
# 10.1.197.145:8999


def validate(date_str):
    try:
      datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
      raise ValueError("Incorrect data format, should be YYYYMMDD")
      exit()



def last_day_of_month(any_day):
    next_month = datetime.strptime((any_day[0:6]+'28'),'%Y%m%d') + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

def flytxt_pqualified_msisdn():
    main.submodule_name="flytxt_pqualified_msisdn" #name your module for debugging
    print ('Main start date in Loop :', int(main.start_date))
    for loop_date in range(int(main.start_date), int(main.end_date)+1):
    #for loop_date in range(int(main.start_date), int(main.end_date)):
        main.this_date = str(loop_date)
        print("Current Date :",main.this_date)
        try:
           sql_stmnt = """presto --server """ + main.presto_ip_port +"""  --catalog hive5 --schema flare_8 --output-format CSV --client-tags """ + main.presto_username +"""  --execute "
           SET SESSION query_max_stage_count=2000;
           start transaction;
           delete from nigeria.flytxt_pqualified_msisdn where date_key = """ + main.this_date +""" ;
           insert into nigeria.flytxt_pqualified_msisdn (
           select msisdn,cast(date_format(date_key,'%Y%m%d') as int)  date_key
           from development.postpaid_creditscore
           where avg_rev >= 5000
           and cs_based_on_avg >= 4
           and date_key in (select max(date_key) from development.postpaid_creditscore)
           and try_cast(substring( cast(date_format(date_key,'%Y%m%d')  as varchar), 1, 6) as int) =
           try_cast(date_format(date_trunc('month', date_add('month', -1, date_parse('""" + main.this_date +"""','%Y%m%d'))), '%Y%m') as int)
           );
           ;
           commit;" """
           print(sql_stmnt)
           startTime = time.time()
           #endTime = time.time()
           os.system(sql_stmnt)
           time_dif = time.time() - startTime
           print(datetime.now().strftime("%d-%m-%Y %H:%M:%S"),"Script name: "+main.script_name+" Module name: "+main.module_name+" Submodule: "+main.submodule_name+"[", str(main.start_date))
           print("] Completed ",datetime.now().strftime("%d-%m-%Y %H:%M:%S")," Elapsed Time(s) " , round(time_dif,2))
           sys.stdout.write('\n') ; sys.stdout.flush();
        except (IOError,ValueError,TypeError,RuntimeError,DatabaseError) as e:
           print ("Unexpected error",e.errno)
           return
        except:        #Note the use of except here,without any exception class, this can be used to handle all exception classes.
          print ("Unexpected error")
          return
    #print("Repairing table .....")
    #os.system('beeline -n daasuser -e "msck repair table nigeria.flytxt_pqualified_msisdn"')
#end module 1


def main():
  #thh = conn_config.presto_db2()
  #serv, puser = conn_config.presto_db2()
  #print('Presto Server', thh.serv)
  #print('Presto User', thh.puser)

# clear screen
  main.module_name='Set module name'
  main.script_name=sys.argv[0]
  main.submodule_name= 'set the sub module name in submodule'
  myCmd = 'clear'
  os.system(myCmd)


  #print ('Presto Server:', cfg.presto_db['presto_server'])
  #print ('Presto Server:', cfg.presto_db['presto_user'])
  main.presto_ip_port = cfg.presto_db['presto_server']
  main.presto_username=cfg.presto_db['presto_user']


  curr_hr = int(datetime.now().strftime("%H")) ## Derive current hour

  sys.stdout.flush();

  today = datetime.today().strftime('%Y%m%d')
  main.start_date = (datetime.today()+ timedelta(days=(-1))).strftime('%Y%m%d')
  prev_day = (datetime.today() - timedelta(days=(-2))).strftime('%Y%m%d')


  if (len(sys.argv)) >= 2:
     main.start_date=sys.argv[1]
  main.end_date = main.start_date
  if (len(sys.argv)) >= 3:
     main.end_date=sys.argv[2]
## validate end date and start date
  validate(main.start_date)
  validate(main.end_date)
  if (int(main.end_date)-int(main.start_date)) > 31:
     print ('Start and End Date must be within the same month', main.start_date, ': ', main.end_date)
     exit()
  main.firstdate_ofmonth = main.start_date[0:6]+'01'
  main.lastdate_ofmonth = (last_day_of_month(main.firstdate_ofmonth)).strftime('%Y%m%d')
  main.this_date  = main.start_date

  #########################user for debugging only, remove after, for final production copy ###############################
  print ('Start Date :', main.start_date)
  print ('end  Date :', main.end_date)
  print ('firstdate_ofmonth :', main.firstdate_ofmonth)
  print ('lastdate_ofmonth :', main.lastdate_ofmonth)
  print ('Today :', today)
  #next_month - datetime.timedelta(days=(-1))
  print ('Start Date :', main.start_date)
  print ('Current Hour :', curr_hr)
  #########################################################################################################################
  ###############################################################
  #### Call module here to perform your SQL operations###########
  ###############################################################
  flytxt_pqualified_msisdn()

  print("Repairing table .....")
  os.system('beeline -n daasuser -e "msck repair table nigeria.flytxt_pqualified_msisdn"')
  sys.stdout.flush();

# Call function to loop for not of days defined
if __name__ == "__main__":
    main()


