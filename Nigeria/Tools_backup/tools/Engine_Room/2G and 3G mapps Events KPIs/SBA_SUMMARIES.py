#! /usr/bin/env /usr/local/bin/python3.6
import sys
import getopt
import os
import subprocess
import re

import csv

from datetime import datetime
from datetime import timedelta
from calendar import monthrange
from pyhive import presto
import cx_Oracle
import time

import logging
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import subprocess
requests.packages.urllib3.disable_warnings()

def param(argv):  
    try:
      print('Syntax  month_end_reports.py -s <yyyy-mm-dd> -l <N> -p <ALL>')
      opts, args = getopt.getopt(argv,"hs:l:p:",["sdate=","itration=","psub="])
      #print(opts,args)
      main.loop_cnt  = 3
      main.StartDate = (datetime.today()+ timedelta(days=(-0))).strftime('%Y-%m-%d')
      main.kpi       = 'ALL'
    except getopt.GetoptError:
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print ('month_end_reports.py -s <yyyy-mm-dd> -l <N> -p <ALL>')
         sys.exit()
      elif opt in ("-s", "--sdate"):
          main.StartDate = arg
      elif opt in ("-l", "--itration"):
         main.loop_cnt = int(arg)
      elif opt in ("-p", "--psub"):
         main.kpi = arg.upper()
     
def loop_fc():
    proc_date = datetime.strptime(main.StartDate, '%Y-%m-%d') + timedelta(days=(-0))
    prev_date = datetime.strptime(main.StartDate, '%Y-%m-%d') + timedelta(days=(-1))
    curr_day  = datetime.today().day
   
    print('Day of the month     :',curr_day)
    
    #if int(curr_day) in (1,2,3,5,6,7,8):
     #   main.loop_cnt = 3
        
    print ('Cycle                :', main.loop_cnt)
        
    for num in range(0,int(main.loop_cnt)):
                
        main.run_date      = proc_date.strftime('%Y%m%d')
        main.run_prev_date = prev_date.strftime('%Y%m%d')
        main.run_day       = proc_date.strftime('%d')
        
        #print("Current Date          :",main.run_date)

        try:      
           pr_date()
           
           if main.kpi == "HOURLY_POSTPAID":           
              hourly_recharge_postpaid()
           if main.kpi == "ISEC":           
              Isec_balance_monitor()

           if main.kpi == "NETWORK_2G_EVENTS":
              network_2g_events()

           if main.kpi == "NETWORK_3G_EVENTS":
              network_3g_events()
    
           proc_date = proc_date  + timedelta(days=-1)
           prev_date = prev_date  + timedelta(days=-1)
           
        except (IOError,ValueError,TypeError,RuntimeError,DatabaseError) as e:
           print ("Unexpected error",e.errno)
           return
        except:        #Note the use of except here,without any exception class, this can be used to handle all exception classes.
          print ("Unexpected error")
          return

def pr_date(): 
    sql_stmnt =  """select  
					date_format(date_add('day', -0, date_trunc('month', date_add('day', -0, date_parse('""" + str(main.run_date) +  """','%Y%m%d')))), '%Y%m%d'),
					date_format(date_add('day', -1, date_trunc('month', date_add('month', 1, date_parse('""" + str(main.run_date) +  """','%Y%m%d')))), '%Y%m%d'),
					date_format(date_add('day', -0, date_trunc('month', date_add('year', -1, date_parse('""" + str(main.run_date) +  """','%Y%m%d')))), '%Y%m%d'),
					date_format(date_add('day',-1, date_add('month', 1, date_add('day', -0, date_trunc('month', date_add('year', -1, date_parse('""" + str(main.run_date) +  """','%Y%m%d')))))), '%Y%m%d'),
					date_format(date_trunc('month',date_add('day', -1, date_trunc('month', date_add('day', -0, date_parse('""" + str(main.run_date) +  """','%Y%m%d'))))),'%Y%m%d'),
					date_format(date_add('day', -1, date_trunc('month', date_add('day', -0, date_parse('""" + str(main.run_date) +  """','%Y%m%d')))), '%Y%m%d'),
					date_format(date_trunc('month',date_add('day', -1, date_trunc('month', date_add('day', -0, date_parse('""" + str(main.run_date) +  """','%Y%m%d'))))),'%Y%m'),
					date_format(date_add('day', -0, date_trunc('month', date_add('year', -1, date_parse('""" + str(main.run_date) +  """','%Y%m%d')))), '%Y%m')"""
    #print(sql_stmnt)
    
    conn       = presto.connect(host='10.1.197.145',port=8999,protocol='http',catalog='hive5',schema='nigeria',username='monthlyrep') 
    cursor     = conn.cursor()
    cursor.execute(sql_stmnt)
    
    for row in cursor.fetchall():
        main.first_date_of_rundate		=	row[0]
        main.last_date_of_rundate		  =	row[1]
        main.start_YonY		            =	row[2]
        main.end_YonY		              =	row[3]
        main.prev_month_start_date		=	row[4]
        main.prev_month_end_date	    =	row[5]
        main.prev_month            		=	row[6]+main.run_day   
        main.prev_year            		=	row[7]+main.run_day 

        main.fin_prd  = str(row[5])[0:6]
        main.cur_prd  = str(row[1])[0:6]
        main.cmonth   = str(row[1])[4:6] 
        main.cyear    = str(row[1])[0:4]        
               
    days_in_month(int(main.cmonth), int(main.cyear))
    
    print("Start of Run Date     :",main.first_date_of_rundate)
    print("End of Run Date       :",main.last_date_of_rundate)
    print("Prev Month Start Date :",main.prev_month_start_date)
    print("Prev Month End Date   :",main.prev_month_end_date)
    print("Curr Financial Period :",main.cur_prd)
    print("Prev Financial Period :",main.fin_prd)
    print("Financial Month       :",main.cmonth)
    print("Financial Year        :",main.cyear)
    print("No of Days in Month   :",main.loop_cnt)

    sys.stdout.flush();
       
    if (main.cmonth == "01"):
        main.tname = 'Jan'+main.cyear
    elif (main.cmonth == "02"):
        main.tname = 'Feb'+main.cyear
    elif (main.cmonth == "03"):
        main.tname = 'Mar'+main.cyear
    elif (main.cmonth == "04"):
        main.tname = 'Apr'+main.cyear
    elif (main.cmonth == "05"):
        main.tname = 'May'+main.cyear
    elif (main.cmonth == "06"):
        main.tname = 'Jun'+main.cyear
    elif (main.cmonth == "07"):
        main.tname = 'Jul'+main.cyear
    elif (main.cmonth == "08"):
        main.tname = 'Aug'+main.cyear
    elif (main.cmonth == "09"):
        main.tname = 'Sep'+main.cyear
    elif (main.cmonth == "10"):
        main.tname = 'Oct'+main.cyear
    elif (main.cmonth == "11"):
        main.tname = 'Nov'+main.cyear
    elif (main.cmonth == "12"):
        main.name = 'Dec'+main.cyear
        
def leap_year(year):
    if year % 400 == 0:
        return True
    if year % 100 == 0:
        return False
    if year % 4 == 0:
        return True
    return False

def days_in_month(month, year):
    days =  30
    main.loop_cnt =  30
    if month in {1, 3, 5, 7, 8, 10, 12}:
        main.days = 31
        main.loop_cnt =  30
        print("No of Days in Month   : ",main.days)
    if month == 2:
        if leap_year(year):
            main.days =  29
            main.loop_cnt =  29
            print("No of Days in Month   : ",main.days)
        main.days =  28
        main.loop_cnt =  28

  
def hourly_recharge_postpaid():
  main.mydir           = "/nas/share05/ops/mtnops/"
  main.myschema        = "nigeria";
  main.proc_short_desc = "Hourly_recharge_postpaid Report"   
  main.tbl_name        = "nigeria.postpaid_hourly_recharges_by_channel"   
  main.sql_main        = """presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daily --user daily --execute "
start transaction;
SET SESSION query_max_stage_count=2000;
delete from """ + main.tbl_name + """ 
where tbl_dt = """ + str(main.run_date) + """ ;
insert into """ + main.tbl_name + """
select
	MSISDN_KEY
    ,hr Hour
   , "round"("sum"((CASE WHEN ((event_type = 'VOUCHER') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) Voucher
   , "round"("sum"((CASE WHEN ((event_type = 'SDP VTU')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) VTU
   , "round"("sum"((CASE WHEN ((event_type = 'DYA') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) DYA_RECHARGE
   , "round"("sum"((CASE WHEN ((event_type = 'DEMAND') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) MOD_RECHARGE
   , "round"("sum"((CASE WHEN ((event_type IN ('MOD FASTLINK', 'MOD FASTLINK HOTDEALS')) AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) MOD_EXTRA_VALUE_DATA
   , "round"("sum"((CASE WHEN ((event_type = 'MOD VOICE') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) MOD_EXTRA_VALUE_VOICE
   , "round"("sum"((CASE WHEN ((event_type = 'MOD DATA') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) MOD_BUNDLE_PURCHASE
   , "round"("sum"((CASE WHEN ((event_type = 'APPRECHG') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) SMARTAPP_RECHARGE
   , "round"("sum"((CASE WHEN ((event_type IN ('DATA RESET', 'BROADBAND DATA RESET')) AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) DATARESET_RECHARGE
   , "round"("sum"((CASE WHEN ((event_type = 'SP ECW&DYA') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) DYA_BUNDLE_PURCHASE
   , "round"("sum"((CASE WHEN (("upper"(event_type) LIKE '%SMART%') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) SMARTAPP_BUNDLE_PURCHASE
   , "round"("sum"((CASE WHEN ((event_type = 'SP Data Reset') AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) DATARESET_BUNDLE_PURCHASE
   , "round"("sum"((CASE WHEN ((event_type IN ('VOUCHER', 'SDP VTU', 'DYA', 'DEMAND', 'MOD FASTLINK', 'SP Data Reset', 'SP ECW&DYA', 'SMARTAPP', 'SP Smart APP', 'MOD VOICE', 'MOD DATA', 'APPRECHG', 'DATA RESET', 'BROADBAND DATA RESET', 'MOD FASTLINK HOTDEALS')) AND (product_type = 'RECHARGES')) THEN COALESCE(TRY_CAST(amount AS double), 0) ELSE 0 END)), 0) Total_Recharges
   , service_class_id
   ,date_key tbl_dt
   FROM
  nigeria.daas_daily_usage_by_msisdn p
WHERE date_key = """ + str(main.run_date) + """
and subscriber_type = 'POSTPAID'
and hr is not null
group by hr,date_key,service_class_id,MSISDN_KEY;
commit;" """
 
  print ('Report Name           :', main.proc_short_desc)
  print ('SQL                   :', main.sql_main)
  sys.stdout.flush();
  
  os.system(main.sql_main)
  
  print(main.proc_short_desc, str(main.fin_prd), end = '')
  print("] Completed ")    
  sys.stdout.write('') ; sys.stdout.flush(); 
  
  
def Isec_balance_monitor():
  main.mydir           = "/nas/share05/ops/mtnops/"
  main.myschema        = "nigeria";
  main.proc_short_desc = "Isec_Monitor_Balance"   
  main.tbl_name        = "nigeria.Isec_balance"   
  main.sql_main        = """presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daily --user sadamson --execute "
start transaction;
SET SESSION query_max_stage_count=2000;
delete from """ + main.tbl_name + """ 
where tbl_dt = """ + str(main.run_date) + """ ;
insert into """ + main.tbl_name + """ (transaction_date,service_class,channel_name,code_of_channel,subscriber_msisdn,transaction_status,transaction_amount,transaction_id,tbl_dt)
select 
tbl_Dt transaction_date,
permanentserviceclass service_class,
originhostname channel_name,
parametervalue code_of_channel,
'234'||accountnumber subscriber_msisdn,
'SUCCESS' Transaction_status,
cast(transactionamount as double)transaction_amount,
origintransactionid transaction_id,
tbl_dt
from FLARE_8.cs5_air_refill_ma
where TBL_DT = """ + str(main.run_date) + """
and externaldata1 in ('2340110010988','2340110010986');
commit;" """
 
  print ('Report Name           :', main.proc_short_desc)
  print ('SQL                   :', main.sql_main)
  sys.stdout.flush();
  
  os.system(main.sql_main)
  
  print(main.proc_short_desc, str(main.fin_prd), end = '')
  print("] Completed ")    
  sys.stdout.write('') ; sys.stdout.flush();


def network_2g_events():
  main.mydir           = "/nas/share05/ops/mtnops/"
  main.myschema        = "engine_room";
  main.proc_short_desc = "engine_network_2g_events"   
  main.tbl_name        = "engine_room.network_2g_events"   
  main.sql_main        = """presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daily --user sadamson --execute "
start transaction;
SET SESSION query_max_stage_count=2000;
delete from """ + main.tbl_name + """ 
where tbl_dt = """ + str(main.run_date) + """ ;
insert into """ + main.tbl_name + """ (date, cellid, cell_id, provider, traffic_hr, traffic_fr, tch_block, tch_drop, tch_attempt, cell_capacity, tbl_dt)
select
"date",
cellid,
cell_id,
provider,
traffic_hr,
traffic_fr,
tch_block,
tch_drop,
tch_attempt,
cell_capacity,
tbl_dt
from engine_room.vp_network_2g_events
where tbl_dt=""" + str(main.run_date) + """;
commit;" """
 
  print ('Report Name           :', main.proc_short_desc)
  print ('SQL                   :', main.sql_main)
  sys.stdout.flush();
  
  os.system(main.sql_main)
  
  print(main.proc_short_desc, str(main.fin_prd), end = '')
  print("] Completed ")    
  sys.stdout.write('') ; sys.stdout.flush();
  
def network_3g_events():
  main.mydir           = "/nas/share05/ops/mtnops/"
  main.myschema        = "engine_room";
  main.proc_short_desc = "engine_network_3g_events"
  main.tbl_name        = "engine_room.network_3g_events"
  main.sql_main        = """presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daily --user sadamson --execute "
start transaction;
SET SESSION query_max_stage_count=2000;
delete from """ + main.tbl_name + """
where tbl_dt = """ + str(main.run_date) + """ ;
insert into """ + main.tbl_name + """ (date, data_rrc_setup_success, data_rrc_setup_total, data_rab_setup_success, data_rab_setup_total, voice_rrc_setup_success, voice_rrc_setup_total, voice_rab_setup_success, voice_rab_setup_total, data_drop, data_setup_completed, voice_drop, voice_setup_completed, data_downlink_volume, provider, cellid, cell_id, tbl_dt)
select
"date",
data_rrc_setup_success,
data_rrc_setup_total,
data_rab_setup_success,
data_rab_setup_total,
voice_rrc_setup_success,
voice_rrc_setup_total,
voice_rab_setup_success,
voice_rab_setup_total,
data_drop,
data_setup_completed,
voice_drop,
voice_setup_completed,
data_downlink_volume,
provider,
cellid,
cell_id,
tbl_dt
from engine_room.vp_network_3g_events
where tbl_dt=""" + str(main.run_date) + """;
commit;" """

  print ('Report Name           :', main.proc_short_desc)
  print ('SQL                   :', main.sql_main)
  sys.stdout.flush();

  os.system(main.sql_main)

  print(main.proc_short_desc, str(main.fin_prd), end = '')
  print("] Completed ")
  sys.stdout.write('') ; sys.stdout.flush(); 

 
def main():
# clear screen
  myCmd = 'clear'
  os.system(myCmd)
  
  cmd = ['ps aux | grep -i "/usr/bin/python3.6 /nas/share05/ops/mtnops/python/data_ingestion.py" | grep -v "grep" | wc -l']
  process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
  stderr=subprocess.PIPE) 
  my_pid, err = process.communicate()

  if int(subprocess.call(cmd, shell=True)) >= 1:
     print("Daily Usage Summary script is currently Running, terminate current job and resubmit ")
     exit()
  
# Initialise variable
  StartDate     = ''
  loop_cnt      = '3'
  kpi           = ''
  curr_hr       = ''
  run_date      = ''
  run_prev_date = ''
  run_day       = ''
  kpi_name      = ''
  fin_prd       = ''
  cur_prd       = ''
  cmonth        = ''
  cyear         = ''
  tname         = ''
  sid           = ''
  service_name  = ''
  user_id       = ''
  host          = ''
  port          = ''
  pswd          = ''
  dsn_tns       = ''
  conn          = ''
  days          = 30
  month         = ''
  year          = '' 
  cmonth        = ''
  cyear         = ''

 
  sql_main        = '' 
  mydir           = ''
  myschema        = ''
  proc_short_desc = ''  
  tbl_name        = ''   
  
  param(sys.argv[1:])   # Call Param to derive Start Date, Loop Count and Function to be executed
 
  curr_hr = int(datetime.now().strftime("%H")) ## Derive current hour
  
  if (curr_hr >= 20 and curr_hr <= 23 and main.loop_cnt < 16):
     main.loop_cnt = 3    

  loop_fc()                  # Call function to loop for not of days defined
    
if __name__ == "__main__":
    main()
