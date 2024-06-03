#!/usr/bin/env perl

## Report Description 
## Template to be used to execute report to be stored in a partition table
## The table must created first separate statement on presto or hive, preferrably presto
## Replace the first two lines with description of the report/module
## 
## Developer Names:
## Support Personnel Name
## Date Last modified
## Change History with Dates

##Parameters: Start Date and End Date in that order

#use strict;
use FileHandle;
use Getopt::Long;
use IO::Handle;
use IO::Socket;

## Advance variables start
## all variables in this enclosure are for advance 
my @months      = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my @months      = qw(01 02 03 04 05 06 07 08 09 10 11 12);
my @weekDays    = qw(Sun Mon Tue Wed Thu Fri Sat Sun);
my @dates;

my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
my $year = 1900 + $yearOffset;
my $theTime = "$hour:$minute:$second, $weekDays[$dayOfWeek] $months[$month] $dayOfMonth, $year";
my $epoc = time();
my $check_sql;
## Advance variables end

my $rec_cnt;

##parameter from command lines.
my $values = @ARGV;
my $value_cnt=scalar $values;
my $start_date   = $ARGV[0];
my $end_date   = $ARGV[0];
my $max_exe_tries = 2 ;
my $cnt_exe = 0 ;
my $rec_cnt;
my $rec_cnt2;

##-----set variable values here 
## $sql_main - the sql statement to be executed should be assigned to this later in the code below the declaration section
## $hive_root  - hive root is set here for usage, there may be no need to change this unless you have to 
## $dir  - this is set to normal/planned ops directory for the report execution, it will be set but can be changed depending
## $schema - schema name, preset to nigeria, the planned schema for operational opco reporting


## These (below) definitely must be changed
my $proc_name = "Monthly_Solo_Script.pl" ; #add script name here, change to appropriate name
my $proc_short_desc = "Monthly_Solo_Script" ; #Short Script Description here pls


{
system('clear');

##check if this module/report is already running and stop the current execution if yes
my $pc = `ps -ef |$proc_name|wc -l`;
$pc =~ s/^\s+//;  ## strip leading and trailing spaces
chomp($pc);      
 
if ($pc gt 3 ) { print "\n$proc_short_desc job is currently running\n"; exit; }  ## exit if sample job is already running

if ($value_cnt gt 1 ) {
    $end_date = $ARGV[1];
 }

print "\nStarting Trend\n";
print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');

##the loop to execute a date at a time 
##note that start and end date must be within a month, the process cannot handle dates across months


for ($proc_date=$start_date; $proc_date <=$end_date; $proc_date++) 
{
####AGGREGATION

print "\nRGS30 collection\n";

$sql_stmt_del = "
delete from ng_ops_support.solo_rgs30_base
;";

$sql_stmt = "
insert into ng_ops_support.solo_rgs30_base
select msisdn_key
from flare_8.customersubject
where tbl_dt = $proc_date
and service_class_id not in('58','70','35') 
and bartype <> 'barred' 
and portstatus = 'NA' 
and rgs30 = 1
and aggr = 'daily'
;";


#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_rgs30_base table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_rgs30_base table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_rgs30_base\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_rgs30_base";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part2 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(msisdn_key) cnt 
	  from flare_8.customersubject 
	  where tbl_dt = $proc_date 
	  and service_class_id not in('58','70','35') 
	  and bartype <> 'barred' 
	  and portstatus = 'NA' 
	  and rgs30 = 1 
	  and aggr = 'daily' ";
	  
	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation




################5b5 MON

print "\nTable solo_data_kpi_5b5mon population\n";

$sql_stmt_del = "
delete from ng_ops_support.solo_data_kpi_5b5mon
;";

$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5mon 
select 
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then month_key end) as Report_month,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then imsi end) as SIM_ID,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then imei end) as Billing_Phone_ID,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then makemodel end) as Phone_handset_model,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then siteid end) as Beacon_ID,
msisdn_key as Billing_phone_Number,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then act_date end) as Phone_activation_date,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then areaname end) as Cust_Address_City,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then lga end) as Cust_Address_LGA,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then try_cast(substr(try_cast(month_key as varchar),1,4) as int) - try_cast(substr(dob,1,4) as int) end) as Customer_Age,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then gender end) as Customer_Gender,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then ngstate end) as Cust_Address_State,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then sms_count end) as outbound_SMS_Count_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then sms_count end) as outbound_SMS_Count_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then sms_count end) as outbound_SMS_Count_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then sms_in_count end) as inbound_SMS_Count_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then sms_in_count end) as inbound_SMS_Count_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then sms_in_count end) as inbound_SMS_Count_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then sms_cost end) as Total_SMS_Spend_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then sms_cost end) as Total_SMS_Spend_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then sms_cost end) as Total_SMS_Spend_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then vol_mb end) as Data_Usage_in_MB_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then vol_mb end) as Data_Usage_in_MB_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then vol_mb end) as Data_Usage_in_MB_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then data_rev end) as Data_Spend_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then data_rev end) as Data_Spend_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then data_rev end) as Data_Spend_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then (voi_onnet_in_counter+voi_offnet_in_counter+voi_int_in_counter) end) as Inbound_Call_Count_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then (voi_onnet_in_counter+voi_offnet_in_counter+voi_int_in_counter) end) as Inbound_Call_Count_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then (voi_onnet_in_counter+voi_offnet_in_counter+voi_int_in_counter) end) as Inbound_Call_Count_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then no_of_calls end) as Outbound_Call_Count_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then no_of_calls end) as Outbound_Call_Count_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then no_of_calls end) as Outbound_Call_Count_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then (voi_onnet_in_mou+voi_int_in_mou) end) as Inbound_Call_Duration_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (voi_onnet_in_mou+voi_int_in_mou) end) as Inbound_Call_Duration_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (voi_onnet_in_mou+voi_int_in_mou) end) as Inbound_Call_Duration_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then mou end) as Outbound_Call_Duration_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then mou end) as Outbound_Call_Duration_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then mou end) as Outbound_Call_Duration_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then voice_rev end) as Outbound_call_Spend_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then voice_rev end) as Outbound_call_Spend_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then voice_rev end) as Outbound_call_Spend_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then tot_rev end) as Total_spend_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then tot_rev end) as Total_spend_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then tot_rev end) as Total_spend_6M,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then last_recharge_dt end) as Most_recent_recharge_date,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then last_data_bundle_dt end) as Most_recent_Bundle_Purchase_date,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then last_data_bundle_dt end) as Most_recent_Data_Bundle_Purchase_date,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then social_vol end) as Social_Network_Data_Usage,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then bonus_voice_rev end) as Bonus_Amount_Calls_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then bonus_voice_rev end) as Bonus_Amount_Calls_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then bonus_voice_rev end) as Bonus_Amount_Calls_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then bonus_mou end) as Bonus_Calls_Duration_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then bonus_mou end) as Bonus_Calls_Duration_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then bonus_mou end) as Bonus_Calls_Duration_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then bonus_sms_cost end) as Bonus_Amount_SMS_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then bonus_sms_cost end) as Bonus_Amount_SMS_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then bonus_sms_cost end) as Bonus_Amount_SMS_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then data_da_bonus end) as Bonus_Amount_Data_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then data_da_bonus end) as Bonus_Amount_Data_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then data_da_bonus end) as Bonus_Amount_Data_6M,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then (case when subscriber_type = 'POSTPAID' then 1 else 0 end) end) as Post_paid_frag,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then dola end) as GSM_LA_DAYS_QTY,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then (case when lte_4g_ind = 1 then '4G' when lte_4g_ind = 0 and nw_3g_ind = 1 then '3G' else '2G' end) end) as Network_Type,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then total_rgs30 end) as RGS_30_flag,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then extra_time end) as Xtratime_Value_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then extra_time end) as Xtratime_Value_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then extra_time end) as Xtratime_Value_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then extra_val_amt end) as Xtrabytes_Value_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then extra_val_amt end) as Xtrabytes_Value_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then extra_val_amt end) as Xtrabytes_Value_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then month_key end) as Date_of_Data_extraction,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then smartphone end) as smart_phone_flag,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then number_of_sim end) as Flag_of_dual_sim,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
--then (extra_time_loan+extra_byte_loan) end) as Value_of_airtime_Loans_received_6M,
then (extra_time_loan) end) as Value_of_airtime_Loans_received_6M,
avg(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
--then (extra_time+extra_val_amt) end) as average_value_of_airtime_loan_6M,
then (extra_time_loan) end) as average_value_of_airtime_loan_6M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (case when vol_mb > 0 then 1 else 0 end) end) as Consistency_in_data_bundle_usage_in_6M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then vas_rev end) as Total_subscriptions_Value_6m
from nigeria.segment5b5_mon a
where tbl_dt in 
(
try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',+1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
)
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key
;";

print "\nValidation check on Table solo_data_kpi_5b5mon\n";

#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_kpi_5b5mon table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_kpi_5b5mon table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_kpi_5b5mon\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_data_kpi_5b5mon where report_month = try_cast(substr(try_cast($proc_date as varchar),1,6) as int) ";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct msisdn_key) cnt from nigeria.segment5b5_mon a  
	  where tbl_dt in (
		try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',+1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
		try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
		try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
		try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
		try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
		try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
		)
		and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key) ";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 60 Secs\n";
        sleep 10;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation






#########################
#########################BSL


print "\nTable solo_data_kpi_bsl population\n";

$sql_stmt_del = "
delete from ng_ops_support.solo_data_kpi_bsl
;";

$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_bsl 
select 
msisdn_key,
max(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then first_usage_date end) as Network_activation_date,
avg(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then bal_avg_daily end) as Average_airtime_Balance_1M,
avg(case when month_key in 
(
try_cast(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then bal_avg_daily end) as Average_airtime_Balance_3M,
avg(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then bal_avg_daily end) as Average_airtime_Balance_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then (case when bal_last_day_period < 20 then 1 else 0 end) end) as GSM_DAB_QTY_20_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (case when bal_last_day_period < 20 then 1 else 0 end) end) as GSM_DAB_QTY_20_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (case when bal_last_day_period < 20 then 1 else 0 end) end) as GSM_DAB_QTY_20_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then (case when bal_last_day_period < 10 then 1 else 0 end) end) as GSM_DAB_QTY_10_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (case when bal_last_day_period < 10 then 1 else 0 end) end) as GSM_DAB_QTY_10_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (case when bal_last_day_period < 10 then 1 else 0 end) end) as GSM_DAB_QTY_10_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then balance_transfer_in_cnt end) as GSM_ME2U_RECEIVED_QTY_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then balance_transfer_in_cnt end) as GSM_ME2U_RECEIVED_QTY_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then balance_transfer_in_cnt end) as GSM_ME2U_RECEIVED_QTY_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then balance_transfer_in_rev end) as GSM_ME2U_RECEIVED_AMT_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then balance_transfer_in_rev end) as GSM_ME2U_RECEIVED_AMT_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)  
then balance_transfer_in_rev end) as GSM_ME2U_RECEIVED_AMT_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then balance_transfer_out_cnt end) as GSM_ME2U_SEND_QTY_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then balance_transfer_out_cnt end) as GSM_ME2U_SEND_QTY_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then balance_transfer_out_cnt end) as GSM_ME2U_SEND_QTY_6M,
sum(case when month_key = try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) then balance_transfer_out_rev end) as GSM_ME2U_SEND_AMT_1M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
then balance_transfer_out_rev end) as GSM_ME2U_SEND_AMT_3M,
sum(case when month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then balance_transfer_out_rev end) as GSM_ME2U_SEND_AMT_6M
from flare_8.customersubject a
where month_key in 
(
try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
and aggr = 'monthly'
group by msisdn_key
;";


#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_kpi_bsl table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_kpi_bsl table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_kpi_bsl\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_data_kpi_bsl";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct msisdn_key) cnt 
	  from flare_8.customersubject a  
	  where month_key in 
	 (
	try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
	try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
	try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
	try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
	try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
	try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
	 ) 
	and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
	and aggr = 'monthly' 
	";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation







#############
############RECHARGE 

$sql_stmt_del = "
delete from ng_ops_support.solo_recharge_5b5_rch
;";


$sql_stmt = "
insert into ng_ops_support.solo_recharge_5b5_rch
select 
max(case 
when tbl_dt = try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int) 
then substr(try_cast(tbl_dt as varchar),1,6) 
end) Report_month,
msisdn_key,
sum(case 
when tbl_dt = try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int) 
then events 
end) Number_of_Recharges_1M,
sum(case 
when tbl_dt in
(
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
) then events 
end) Number_of_Recharges_3M,
sum(case 
when tbl_dt in
(
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
) then events 
end) Number_of_Recharges_6M,
sum(case 
when tbl_dt = try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int) 
then amount 
end) Value_of_airtime_Recharges_1M,
sum(case 
when tbl_dt in
(
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
) then amount 
end) Value_of_airtime_Recharges_3M,
sum(case 
when tbl_dt in
(
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
) then amount 
end) Value_of_airtime_Recharges_6M,
max(case 
when tbl_dt = try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int) 
then preferred_channel 
end) GSM_TOPUP_METHOD
from nigeria.segment5b5_rch a
where tbl_dt in
(
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
)
and aggr = 'monthly'
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key
;";

#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_recharge_5b5_rch table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_recharge_5b5_rch table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_recharge_5b5_rch\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_recharge_5b5_rch";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct msisdn_key) cnt 
	  from nigeria.segment5b5_rch a
	where tbl_dt in
	(
	try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
	try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
	try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
	try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
	try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int),
	try_cast(Date_format(date_add('day', -1,date_parse(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
	)
	and aggr = 'monthly'
	and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
	";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation



#######Here Validation done on Daily scripts

print "\nPopulating table solo_uniq_called_tmp3\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_tmp3
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_tmp3
select 
msisdn_key,
sum(unique_num_contacted_all) unique_num_contacted_all,
sum(unique_num_contacted_outbound_calls) unique_num_contacted_outbound_calls,
sum(unique_num_contacted_incoming_calls) unique_num_contacted_incoming_calls,
sum(unique_num_sms_all) unique_num_sms_all,
sum(unique_num_contacted_outbound_sms) unique_num_contacted_outbound_sms,
sum(unique_num_contacted_inbound_sms) unique_num_contacted_inbound_sms,
try_cast(month as int) month
from ng_ops_support.solo_uniq_called_tmp2
where month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



### Aggreating VOICE
print "\nPopulating table solo_uniq_called_voice_tmp3a\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3a
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3a
select month,msisdn_key,called_num,sum(call_counts) call_counts,1 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nPopulating table solo_uniq_called_voice_tmp3b\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3b
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3b 
select month,msisdn_key,called_num,sum(call_counts) call_counts,2 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-7,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp3c\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3c
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3c 
select month,msisdn_key,called_num,sum(call_counts) call_counts,3 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-11,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-8,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp3d\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3d 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3d  
select month,msisdn_key,called_num,sum(call_counts) call_counts,4 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-15,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-12,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_voice_tmp3e\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3e 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3e   
select month,msisdn_key,called_num,sum(call_counts) call_counts,5 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-19,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-16,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp3f\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3f 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3f    
select month,msisdn_key,called_num,sum(call_counts) call_counts,6 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-23,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-20,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp3g\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3g 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3g    
select month,msisdn_key,called_num,sum(call_counts) call_counts,7 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-27,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-24,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_voice_tmp3h\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp3h 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp3h    
select month,msisdn_key,called_num,sum(call_counts) call_counts,8 cycle
from ng_ops_support.solo_uniq_called_voice_tmp2a
where tbl_dt between try_cast(Date_format(date_add('DAY',-31,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int) 
and  try_cast(Date_format(date_add('DAY',-28,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;




##############
print "\nPopulating table solo_uniq_called_voice_tmp4\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4
select month,msisdn_key,called_num,sum(call_counts) call_counts,1 cycle
from(
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3a
union all 
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3b 
) 
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4
select month,msisdn_key,called_num,sum(call_counts) call_counts,2 cycle
from(
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3c
union all 
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3d 
) 
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4
select month,msisdn_key,called_num,sum(call_counts) call_counts,3 cycle
from(
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3e 
union all 
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3f   
) 
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4
select month,msisdn_key,called_num,sum(call_counts) call_counts,4 cycle
from(
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3g  
union all 
select month,msisdn_key,called_num,call_counts from ng_ops_support.solo_uniq_called_voice_tmp3h  
) 
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

##########
##########

print "\nPopulating table solo_uniq_called_voice_tmp4a\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4a
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4a 
select month,msisdn_key,called_num,call_counts,cycle,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4 a
where cycle = 1
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4a 
select month,msisdn_key,called_num,call_counts,cycle,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4 a
where cycle = 2
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4a 
select month,msisdn_key,called_num,call_counts,cycle,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4 a
where cycle = 3
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4a 
select month,msisdn_key,called_num,call_counts,cycle,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4 a
where cycle = 4
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_voice_tmp4b\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4b
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4b
select *
from ng_ops_support.solo_uniq_called_voice_tmp4a
where ranking <= 10
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_voice_tmp4c\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4c
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4c 
select month,msisdn_key,called_num,1 cycle,sum(call_counts) call_counts
from ng_ops_support.solo_uniq_called_voice_tmp4b
where cycle in (1,2) 
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4c 
select month,msisdn_key,called_num,2 cycle,sum(call_counts) call_counts
from ng_ops_support.solo_uniq_called_voice_tmp4b
where cycle in (3,4) 
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_voice_tmp4d\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4d
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4d 
select month,msisdn_key,called_num,call_counts,cycle,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4c a
where cycle = 1
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4d 
select month,msisdn_key,called_num,call_counts,cycle,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4c a
where cycle = 2
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_voice_tmp4e\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4e 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4e
select *
from ng_ops_support.solo_uniq_called_voice_tmp4d
where ranking <= 10
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp4f\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4f
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4f  
select month,msisdn_key,called_num,sum(call_counts) call_counts
from ng_ops_support.solo_uniq_called_voice_tmp4e
group by month,msisdn_key,called_num
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp4g\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4g
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4g 
select month,msisdn_key,called_num,call_counts,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp4f
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp4h\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4h
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4h
select *
from ng_ops_support.solo_uniq_called_voice_tmp4g
where ranking <= 3
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_called_voice_tmp4i\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp4i
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp4i
select month,msisdn_key,sum(call_counts) top_3_call_counts
from ng_ops_support.solo_uniq_called_voice_tmp4h
group by month,msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



#########
######### Aggreating SMS 
print "\nPopulating table solo_uniq_called_sms_tmp3\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_sms_tmp3 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_sms_tmp3
select month,msisdn_key,called_num,sum(sms_counts) sms_counts
from ng_ops_support.solo_uniq_called_sms_tmp2
where month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key,called_num 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nPopulating table solo_uniq_called_sms_tmp4\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_sms_tmp4 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_sms_tmp4
select month,msisdn_key,sum(sms_counts) sms_counts
from(
select month,msisdn_key,called_num,sms_counts,
row_number() over (partition by msisdn_key order by sms_counts desc) ranking
from ng_ops_support.solo_uniq_called_sms_tmp3
) where ranking < 4
group by month,msisdn_key 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


####
print "\nPopulating table solo_uniq_voice_sms_subs\n";
$sql_stmt = "
delete from nigeria.solo_uniq_voice_sms_subs where month = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.solo_uniq_voice_sms_subs  
select 
a.msisdn_key,unique_num_contacted_all,unique_num_contacted_outbound_calls,unique_num_contacted_incoming_calls,unique_num_sms_all,unique_num_contacted_outbound_sms,
unique_num_contacted_inbound_sms,top_3_call_counts call_counts_to_top3_contacted,sms_counts sms_counts_to_top3_contacted,a.month
from ng_ops_support.solo_uniq_called_tmp3 a 
left join ng_ops_support.solo_uniq_called_voice_tmp4i b 
on (a.msisdn_key = b.msisdn_key and a.month = try_cast(b.month as int))
left join ng_ops_support.solo_uniq_called_sms_tmp4 c
on (a.msisdn_key = c.msisdn_key and a.month = try_cast(c.month as int))
where a.month = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
delete from nigeria.solo_uniq_voice_sms_subs where month = try_cast(Date_format(date_add('Month',-10,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPopulating table solo_uniq_voice_sms_subs_tmp\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_voice_sms_subs_tmp
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


####
$sql_stmt = "
insert into ng_ops_support.solo_uniq_voice_sms_subs_tmp
select max(month) reporting_month,
msisdn_key,
sum(case when month = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then unique_num_contacted_all end) as unique_num_contacted_all_1M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
 ) 
 then unique_num_contacted_all end) as unique_num_contacted_all_3M,
sum(case when month = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then unique_num_contacted_outbound_calls end) as unique_num_contacted_outbound_calls_1M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_outbound_calls end) as unique_num_contacted_outbound_calls_3M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_outbound_calls end) as unique_num_contacted_outbound_calls_6M,
sum(case when month = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then unique_num_contacted_incoming_calls end) as unique_num_contacted_incoming_calls_1M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_incoming_calls end) as unique_num_contacted_incoming_calls_3M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_incoming_calls end) as unique_num_contacted_incoming_calls_6M,
sum(case when month = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then unique_num_sms_all end) as unique_num_sms_all_1M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_sms_all end) as unique_num_sms_all_3M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_sms_all end) as unique_num_sms_all_6M,
sum(case when month = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then unique_num_contacted_outbound_sms end) as unique_num_contacted_outbound_sms_1M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_outbound_sms end) as unique_num_contacted_outbound_sms_3M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_outbound_sms end) as unique_num_contacted_outbound_sms_6M,
sum(case when month = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then unique_num_contacted_inbound_sms end) as unique_num_contacted_inbound_sms_1M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_inbound_sms end) as unique_num_contacted_inbound_sms_3M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then unique_num_contacted_inbound_sms end) as unique_num_contacted_inbound_sms_6M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then call_counts_to_top3_contacted end) as call_counts_to_top3_contacted_3M,
sum(case when month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then sms_counts_to_top3_contacted end) as sms_counts_to_top3_contacted_3M
from nigeria.solo_uniq_voice_sms_subs a 
where month in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



######################
###################### RECHARGE DIFF IN DAYS

print "\nPROCESSING table solo_recharge_interv_tmp2 \n";

$sql_stmt = "
delete from ng_ops_support.solo_recharge_interv_tmp2
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

for ($i=0; $i <=30; $i++) 
{
$j=$i+1;

print "\nPROCESSING LOOP $j \n";
print "\nPROCESSING table solo_recharge_interv_tmp2 \n";

$sql_stmt = "
insert into ng_ops_support.solo_recharge_interv_tmp2
select months,msisdn,rech_bef,rech_next,date_diff,rnk
from(
select months,msisdn,rech_bef,rech_next,date_diff,
row_number () over (partition by msisdn,rech_bef order by msisdn,rech_bef,rech_next asc) rnk
from(
select a.months,a.msisdn,a.tbl_dt rech_bef, b.tbl_dt rech_next,
date_diff('day',date_parse(cast(a.tbl_dt as varchar),'%Y%m%d'),date_parse(cast(b.tbl_dt as varchar),'%Y%m%d')) date_diff
from(
select months,tbl_dt,msisdn
from ng_ops_support.solo_recharge_interv_tmp
) a,
(
select months,tbl_dt,msisdn
from ng_ops_support.solo_recharge_interv_tmp
) b 
where a.tbl_dt = try_cast(Date_format(date_add('DAY',-$i,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m%d') as int)
and a.msisdn = b.msisdn and a.tbl_dt < b.tbl_dt
and a.months = substr(try_cast($start_date as varchar),1,6)
)
) where rnk = 1
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;
}


print "\nPROCESSING table solo_recharge_interv_tmp3 \n";

$sql_stmt = "
delete from ng_ops_support.solo_recharge_interv_tmp3
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_recharge_interv_tmp3 
select months,msisdn,sum(date_diff) total_date_diff,count(9) total_date_diff_counts
from ng_ops_support.solo_recharge_interv_tmp2 a 
where exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn = b.msisdn_key) 
group by months,msisdn
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


print "\nPROCESSING table solo_recharge_interval_days \n";

$sql_stmt = "
delete from nigeria.solo_recharge_interval_days where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.solo_recharge_interval_days
select msisdn,total_date_diff,total_date_diff_counts,try_cast(months as int) months
from ng_ops_support.solo_recharge_interv_tmp3
where months = substr(try_cast($start_date as varchar),1,6) 
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



$sql_stmt = "
delete from ng_ops_support.solo_recharge_interval_days_tmp
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_recharge_interval_days_tmp 
select max(months) reporting_month,msisdn,sum(total_date_diff)/sum(total_date_diff_counts) Mean_time_between_recharges
from nigeria.solo_recharge_interval_days
where months between try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
and   try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
group by msisdn
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

################################


############################
############################

##### AVERAGE BALANCE BEFORE RECHARGE

print "\nMONTHLY AGGREGATION OF AVERAGE BALANCE BEFORE RECHARGE \n";

$sql_stmt = "
delete from nigeria.avg_balance_before_recharge where month = substr(try_cast($start_date as varchar),1,6) 
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into nigeria.avg_balance_before_recharge
select msisdn_key,sum(balancebefore_rch) balancebefore_rch,sum(balancebefore_rch_sum) balancebefore_rch_sum,sum(balancebefore_rch_counts) balancebefore_rch_counts,month
from ng_ops_support.xtratime_balance_tmp a 
where month = substr(try_cast($start_date as varchar),1,6) 
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key,month 
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
delete from ng_ops_support.avg_balance_before_recharge_tmp2 
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.avg_balance_before_recharge_tmp2 
select msisdn_key,
sum(case when try_cast(month as int) = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then balancebefore_rch_sum end)/sum(balancebefore_rch_counts) 
as Average_balance_before_recharge_1M,
sum(case when try_cast(month as int) in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then balancebefore_rch_sum end)/sum(balancebefore_rch_counts) as Average_balance_before_recharge_3M,
sum(case when try_cast(month as int) in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then balancebefore_rch_sum end)/sum(balancebefore_rch_counts) as Average_balance_before_recharge_6M
from nigeria.avg_balance_before_recharge
where try_cast(month as int) in
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
group by msisdn_key
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

############################
############################



###########5b5 Usage 

print "\nSolo 5b5 Usage \n";

$sql_stmt = "
delete from nigeria.solo_data_kpi_5b5_usage where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.solo_data_kpi_5b5_usage 
select 
msisdn_key,
sum(data_local_da_bonus_kb) data_local_da_bonus_kb,
sum(data_roam_da_bonus_kb) data_roam_da_bonus_kb,
sum(data_kb) data_kb,
sum(case when data_kb/1024 < 2 then 1 else 0 end) GSM_DAYS_BUND_LESS_2MB,
months
from ng_ops_support.solo_data_kpi_5b5_usg_tmp2 a 
where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key) 
group by months,msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5_usg
select
max(months) Reporting_Moth,
msisdn_key,
sum(case when months = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)  
then (data_local_da_bonus_kb+data_roam_da_bonus_kb)/1024 end) as Bonus_Data_in_MB_1M,
sum(case when months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
 ) 
then (data_local_da_bonus_kb+data_roam_da_bonus_kb)/1024 end) as Bonus_Data_in_MB_3M,
sum(case when months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then (data_local_da_bonus_kb+data_roam_da_bonus_kb)/1024 end) as Bonus_Data_in_MB_6M,
sum(case when months = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)  
then GSM_DAYS_BUND_LESS_2MB end) as GSM_DAYS_BUND_LESS_2MB_1M,
sum(case when months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
 ) 
then GSM_DAYS_BUND_LESS_2MB end) as GSM_DAYS_BUND_LESS_2MB_3M,
sum(case when months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then GSM_DAYS_BUND_LESS_2MB end) as GSM_DAYS_BUND_LESS_2MB_6M
from nigeria.solo_data_kpi_5b5_usage  a
where months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
group by msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


######################
######################


###########5b5 Revenue 

print "\nSolo 5b5 Revenue \n";

$sql_stmt = "
delete from nigeria.solo_data_kpi_5b5_revenue where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



$sql_stmt = "
insert into nigeria.solo_data_kpi_5b5_revenue
select 
msisdn_key,
sum(gsm_active_days_qty) data_local_da_bonus_kb,
max(xtratime_loan) max_airtime_loan,
months
from ng_ops_support.solo_data_kpi_5b5_rev_tmp2 a
where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by months,msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
delete from ng_ops_support.solo_data_kpi_5b5_rev
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5_rev 
select
max(months) Reporting_Moth,
msisdn_key,
sum(case when months = try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int) 
then data_local_da_bonus_kb end) as gsm_active_days_qty_1M,
sum(case when months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
 )  
then data_local_da_bonus_kb end) asgsm_active_days_qty_3M,
sum(case when months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
) 
then data_local_da_bonus_kb end) as gsm_active_days_qty_6M,
max(max_airtime_loan) as maximum_value_of_airtime_loan
from nigeria.solo_data_kpi_5b5_revenue a 
where months in 
(
try_cast(Date_format(date_add('Month',-0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int),
try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
)
group by msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


#####################
#####################

#####SPRINT2
print "\nSPRINT2\n";

###Phone Activation Date 
print "\n Phone Activation Date\n";

$sql_stmt_del = "
delete from ng_ops_support.solo_phone_activation_dt
;";


$sql_stmt = "
insert into ng_ops_support.solo_phone_activation_dt
select tbl_dt,try_cast(msisdn as bigint) msisdn,last_detection	Phone_activation_date	
from flare_8.dmc_dump_all a 
where tbl_dt = try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',+1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
and exists (select * from ng_ops_support.solo_rgs30_base b where try_cast(a.msisdn as bigint) = b.msisdn_key)
;";



#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_phone_activation_dt table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_phone_activation_dt table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_phone_activation_dt\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_phone_activation_dt";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(msisdn_key) cnt 
	  from flare_8.dmc_dump_all a 
where tbl_dt = try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',+1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
and exists (select * from ng_ops_support.solo_rgs30_base b where try_cast(a.msisdn as bigint) = b.msisdn_key)
	";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation





##########Postpaid Limit 

$sql_stmt_del = "
delete from ng_ops_support.solo_postpaid_limit
;";

$sql_stmt = "
insert into ng_ops_support.solo_postpaid_limit 
select tbl_dt,msisdn_key,sum(accumulator_counter) Post_paid_limit 
from flare_8.sdp_dmp_ac a  
where tbl_dt = try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',+1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
and accumulator_id = 41
group by tbl_dt,msisdn_key
;";

#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_postpaid_limit table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_postpaid_limit table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_postpaid_limit\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_postpaid_limit";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct msisdn_key) cnt 
	   from flare_8.sdp_dmp_ac a  
	   where tbl_dt = try_cast(Date_format(date_add('Day',-1,date_parse(Date_format(date_add('Month',+1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')||'01','%Y%m%d')),'%Y%m%d') as int)
	   and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
	   and accumulator_id = 41
	";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation



#####CRBT

print "\nCRBT processing\n";

$sql_stmt_del = "
delete from  nigeria.solo_data_crbt_subscription  where month = substr(try_cast($start_date as varchar),1,6)
;";

$sql_stmt = "
insert into nigeria.solo_data_crbt_subscription 
select msisdn_key,
sum(Number_of_ring_back_tones_subscriptions) Number_of_ring_back_tones_subscriptions,
sum(Vaue_of_ring_back_tones_subscriptions) Vaue_of_ring_back_tones_subscriptions,
month
from ng_ops_support.solo_data_crbt_subscription_tmp
where month = substr(try_cast($start_date as varchar),1,6)
group by month,msisdn_key
;";



#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_crbt_subscription table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_crbt_subscription table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_crbt_subscription\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from nigeria.solo_data_crbt_subscription where month = substr(try_cast($start_date as varchar),1,6) ";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct msisdn_key) cnt 
	   from ng_ops_support.solo_data_crbt_subscription_tmp
	   where month = substr(try_cast($start_date as varchar),1,6)
	";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation


$sql_stmt = "
delete from ng_ops_support.solo_data_crbt_subscription_tmp2
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_data_crbt_subscription_tmp2 
select msisdn_key,
sum(number_of_ring_back_tones_subscriptions) Number_of_ring_back_tones_subscriptions_6M,
sum(vaue_of_ring_back_tones_subscriptions) Vaue_of_ring_back_tones_subscriptions_6M
from nigeria.solo_data_crbt_subscription a 
where month between Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') 
and Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') 
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;




############
##########Xratime and xtrabyte Counts 

$sql_stmt = "
delete from ng_ops_support.solo_extra_time_byte_tmp
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_extra_time_byte_tmp  
select substr(try_cast(tbl_dt as varchar),1,6) month, msisdn_key,count(extratime_loan) extratime_loan_counts,count(extrabyte_loan) extrabyte_loan_counts 
from nigeria.exbyte_extime_comm
where substr(try_cast(tbl_dt as varchar),1,6) between Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') 
and Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') 
group by substr(try_cast(tbl_dt as varchar),1,6), msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
delete from ng_ops_support.solo_extra_time_byte_tmp2
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_extra_time_byte_tmp2 
select msisdn_key,
sum(case when month in 
(
Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')
)
then extratime_loan_counts end) Number_of_airtime_loans_received_6M,
sum(case when month = Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') then extratime_loan_counts end) Xtratime_count_1M,
sum(case when month in 
(
Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')
)
then extratime_loan_counts end) Xtratime_count_3M,
sum(case when month in 
(
Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')
)
then extratime_loan_counts end) Xtratime_count_6M,
sum(case when month = Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') then extrabyte_loan_counts end) Xtrabytes_count_1M,
sum(case when month in 
(
Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')
)
then extrabyte_loan_counts end) Xtrabytes_count_3M,
sum(case when month in 
(
Date_format(date_add('Month', 0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-1,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-2,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-3,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-4,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m'),
Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m')
)
then extrabyte_loan_counts end) Xtrabytes_count_6M
from ng_ops_support.solo_extra_time_byte_tmp a 
where exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



######VAS Counts

$sql_stmt = "
delete from nigeria.solo_data_kpi_subscriptions  where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.solo_data_kpi_subscriptions 
select msisdn_key,sum(Total_subscriptions_count) Total_subscriptions_count,months
from ng_ops_support.solo_data_kpi_subscriptions_tmp
where months = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
group by months,msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



$sql_stmt = "
delete from ng_ops_support.solo_data_kpi_subscriptions_tmp2 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_subscriptions_tmp2  
select msisdn_key,sum(Total_subscriptions_count) Total_subscriptions_count_6m
from nigeria.solo_data_kpi_subscriptions a 
where months between try_cast(Date_format(date_add('Month',-5,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
and try_cast(Date_format(date_add('Month',0,date_parse(try_cast($start_date as varchar),'%Y%m%d')), '%Y%m') as int)
and exists (select * from ng_ops_support.solo_rgs30_base b where a.msisdn_key = b.msisdn_key)
group by msisdn_key
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;





############CUBE
############

$sql_stmt = "
delete from nigeria.solo_data_mart_kpi where report_month = try_cast(substr(try_cast($start_date as varchar),1,6) as int)
;";

$sql_stmt = "
insert into nigeria.solo_data_mart_kpi
select 
 sim_id
,billing_phone_id
,phone_handset_model
,beacon_id
,billing_phone_number
,j.phone_activation_date
,cust_address_city
,cust_address_lga
,customer_age
,customer_gender
,cust_address_state
,outbound_sms_count_1m
,outbound_sms_count_3m
,outbound_sms_count_6m
,inbound_sms_count_1m
,inbound_sms_count_3m
,inbound_sms_count_6m
,total_sms_spend_1m
,total_sms_spend_3m
,total_sms_spend_6m
,data_usage_in_mb_1m
,data_usage_in_mb_3m
,data_usage_in_mb_6m
,data_spend_1m
,data_spend_3m
,data_spend_6m
,inbound_call_count_1m
,inbound_call_count_3m
,inbound_call_count_6m
,outbound_call_count_1m
,outbound_call_count_3m
,outbound_call_count_6m
,inbound_call_duration_1m
,inbound_call_duration_3m
,inbound_call_duration_6m
,outbound_call_duration_1m
,outbound_call_duration_3m
,outbound_call_duration_6m
,outbound_call_spend_1m
,outbound_call_spend_3m
,outbound_call_spend_6m
,total_spend_1m
,total_spend_3m
,total_spend_6m
,most_recent_recharge_date
,most_recent_bundle_purchase_date
,most_recent_data_bundle_purchase_date
,social_network_data_usage
,bonus_amount_calls_1m
,bonus_amount_calls_3m
,bonus_amount_calls_6m
,bonus_calls_duration_1m
,bonus_calls_duration_3m
,bonus_calls_duration_6m
,bonus_amount_sms_1m
,bonus_amount_sms_3m
,bonus_amount_sms_6m
,bonus_amount_data_1m
,bonus_amount_data_3m
,bonus_amount_data_6m
,post_paid_frag
,gsm_la_days_qty
,network_type
,rgs_30_flag
,xtratime_value_1m
,xtratime_value_3m
,xtratime_value_6m
,xtrabytes_value_1m
,xtrabytes_value_3m
,xtrabytes_value_6m
,date_of_data_extraction
,smart_phone_flag
,flag_of_dual_sim
,value_of_airtime_loans_received_6m
,average_value_of_airtime_loan_6m
,consistency_in_data_bundle_usage_in_6m
,total_subscriptions_value_6m
,number_of_recharges_1m
,number_of_recharges_3m
,number_of_recharges_6m
,value_of_airtime_recharges_1m
,value_of_airtime_recharges_3m
,value_of_airtime_recharges_6m
,gsm_topup_method
,network_activation_date
,average_airtime_balance_1m
,average_airtime_balance_3m
,average_airtime_balance_6m
,gsm_dab_qty_20_1m
,gsm_dab_qty_20_3m
,gsm_dab_qty_20_6m
,gsm_dab_qty_10_1m
,gsm_dab_qty_10_3m
,gsm_dab_qty_10_6m
,gsm_me2u_received_qty_1m
,gsm_me2u_received_qty_3m
,gsm_me2u_received_qty_6m
,gsm_me2u_received_amt_1m
,gsm_me2u_received_amt_3m
,gsm_me2u_received_amt_6m
,gsm_me2u_send_qty_1m
,gsm_me2u_send_qty_3m
,gsm_me2u_send_qty_6m
,gsm_me2u_send_amt_1m
,gsm_me2u_send_amt_3m
,gsm_me2u_send_amt_6m
,bonus_data_in_mb_1m
,bonus_data_in_mb_3m
,bonus_data_in_mb_6m
,gsm_days_bund_less_2mb_1m
,gsm_days_bund_less_2mb_3m
,gsm_days_bund_less_2mb_6m
,gsm_active_days_qty_1m
,asgsm_active_days_qty_3m
,gsm_active_days_qty_6m
,maximum_value_of_airtime_loan
,mean_time_between_recharges
,unique_num_contacted_all_1m
,unique_num_contacted_all_3m
,unique_num_contacted_outbound_calls_1m
,unique_num_contacted_outbound_calls_3m
,unique_num_contacted_outbound_calls_6m
,unique_num_contacted_incoming_calls_1m
,unique_num_contacted_incoming_calls_3m
,unique_num_contacted_incoming_calls_6m
,unique_num_sms_all_1m
,unique_num_sms_all_3m
,unique_num_sms_all_6m
,unique_num_contacted_outbound_sms_1m
,unique_num_contacted_outbound_sms_3m
,unique_num_contacted_outbound_sms_6m
,unique_num_contacted_inbound_sms_1m
,unique_num_contacted_inbound_sms_3m
,unique_num_contacted_inbound_sms_6m
,call_counts_to_top3_contacted_3m
,sms_counts_to_top3_contacted_3m
,average_balance_before_recharge_1m
,average_balance_before_recharge_3m
,average_balance_before_recharge_6m
-----Sprint2
post_paid_limit,
number_of_ring_back_tones_subscriptions_6m,
vaue_of_ring_back_tones_subscriptions_6m,
number_of_airtime_loans_received_6m,
xtratime_count_1m,
xtratime_count_3m,
xtratime_count_6m,
xtrabytes_count_1m,
xtrabytes_count_3m,
xtrabytes_count_6m,
total_subscriptions_count_6m
,try_cast(substr(try_cast($start_date as varchar),1,6) as int) report_month
from ng_ops_support.solo_data_kpi_bsl a  
left join ng_ops_support.solo_recharge_5b5_rch b
on (a.msisdn_key = b.msisdn_key)
left join ng_ops_support.solo_data_kpi_5b5mon c 
on (a.msisdn_key = c.billing_phone_number)
left join ng_ops_support.solo_data_kpi_5b5_usg d 
on (a.msisdn_key = d.msisdn_key)
left join ng_ops_support.solo_data_kpi_5b5_rev e 
on (a.msisdn_key = e.msisdn_key)
left join ng_ops_support.solo_recharge_interval_days_tmp f 
on (a.msisdn_key = f.msisdn)
left join ng_ops_support.solo_uniq_voice_sms_subs_tmp g
on (a.msisdn_key = g.msisdn_key)
left join ng_ops_support.avg_balance_before_recharge_tmp2 h 
on (a.msisdn_key = h.msisdn_key)
---------sprint2
left join ng_ops_support.solo_phone_activation_dt j 
on (a.msisdn_key = j.msisdn)
left join ng_ops_support.solo_postpaid_limit k  
on (a.msisdn_key = k.msisdn_key)
left join ng_ops_support.solo_data_crbt_subscription_tmp2 l  
on (a.msisdn_key = l.msisdn_key)
left join ng_ops_support.solo_extra_time_byte_tmp2 m  
on (a.msisdn_key = m.msisdn_key)
left join ng_ops_support.solo_data_kpi_subscriptions_tmp2 o 
on (a.msisdn_key = o.msisdn_key)
;";



#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_mart_kpi table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_mart_kpi table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_mart_kpi\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from nigeria.solo_data_mart_kpi where report_month = try_cast(substr(try_cast($start_date as varchar),1,6) as int)";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part2 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="
	  select count(msisdn_key) counts 
	  from flare_8.customersubject
	  where tbl_dt = $proc_date
	  and service_class_id not in('58','70','35') 
	  and bartype <> 'barred' 
	  and portstatus = 'NA' 
	  and rgs30 = 1
	  and aggr = 'daily'
	  ";

	  $rec_cnt2 =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt2 =~ s/\"//g ; $rec_cnt2 =~ s/cnt//g ; $rec_cnt2 =~ s/[\r\n\r]+//g; chomp($rec_cnt2);
 

print "\nCounts of Base table $rec_cnt2\n";
print "\nCounts of Final Table $rec_cnt\n";

      if ($rec_cnt == $rec_cnt2 )
      {
	print "\nBase table counts is same as Final Table for the day $proc_date\n";
        $cnt_exe = $max_exe_tries;
      }
      else
      {
        print "\nBase table counts is not same as Final Table for the day $proc_date\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
		if ($cnt_exe == $max_exe_tries and $rec_cnt != $rec_cnt2 ) {exit;}
        $cnt_exe++ ;
      }	  
}
#######End Validation



###PURGE TABLES IN DAILY SCRIPT:
print "\nPURGING TABLES\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_tmp2 where 1=2
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp2a  
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_sms_tmp2  
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
delete from ng_ops_support.xtratime_balance_tmp  
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



}

print "\nEND OF PROCESSING \n";
}
