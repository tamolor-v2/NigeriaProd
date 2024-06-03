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
my $rec_cnt2;

##parameter from command lines.
my $values = @ARGV;
my $value_cnt=scalar $values;
my $start_date   = $ARGV[0];
my $end_date   = $ARGV[0];

my $max_exe_tries = 2 ;
my $cnt_exe = 0 ;


## These (below) definitely must be changed
my $proc_name = "Daily_Solo_Script.pl" ; #add script name here, change to appropriate name
my $proc_short_desc = "Daily_Solo_Script" ; #Short Script Description here pls


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

print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');


for ($proc_date=$start_date; $proc_date <=$end_date; $proc_date++) 
{
print "\nPROCESSING FOR $proc_date \n";

###Voice 
print "\nPROCESSING VOICE Part \n";

$sql_stmt_del = "
delete from ng_ops_support.solo_uniq_called 
;";

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called
select substr(try_cast(tbl_dt as varchar),1,6) month,tbl_dt,'call' event_type,
case 
when call_type_enrich = 0 then 'incoming'  
when call_type_enrich = 1 then 'outgoing' 
else 'unknown' end event_type_desc,
chargepartynumber,calledcallingnumber,count(9) counts
from flare_8.cs5_ccn_voice_ma a 
where tbl_dt = $proc_date 
group by tbl_dt,chargepartynumber,calledcallingnumber,
case when call_type_enrich = 0 then 'incoming' when call_type_enrich = 1 then 'outgoing' else 'unknown' end
;";


#######Validation
$cnt_exe = 0 ;
while( $cnt_exe < $max_exe_tries )
{
      print "\nDeletion from solo_uniq_called table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion to solo_uniq_called table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_uniq_called\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_uniq_called where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  
	  $check_sql="
	  select count(9) counts
		from(
		select substr(try_cast(tbl_dt as varchar),1,6) month,tbl_dt,'call' event_type,
			case 
			when call_type_enrich = 0 then 'incoming'  
			when call_type_enrich = 1 then 'outgoing' 
			else 'unknown' end event_type_desc,
			chargepartynumber,calledcallingnumber,count(9) counts
			from flare_8.cs5_ccn_voice_ma a 
			where tbl_dt = $proc_date 
		group by tbl_dt,chargepartynumber,calledcallingnumber,
		case when call_type_enrich = 0 then 'incoming' when call_type_enrich = 1 then 'outgoing' else 'unknown' end
			)
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


#######SMS part 
print "\nPROCESSING SMS Part \n";

$sql_stmt_del = "
delete from ng_ops_support.solo_uniq_called_sms
;";

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_sms
select substr(try_cast(tbl_dt as varchar),1,6) month,tbl_dt,'sms' event_type,
case 
when call_type_enrich = 0 then 'incoming'  
when call_type_enrich = 1 then 'outgoing' 
else 'unknown' end event_type_desc,
chargepartynumber,calledcallingnumber,count(9) counts
from flare_8.cs5_ccn_sms_ma a 
where tbl_dt = $proc_date 
group by tbl_dt,chargepartynumber,calledcallingnumber,
case when call_type_enrich = 0 then 'incoming' when call_type_enrich = 1 then 'outgoing' else 'unknown' end 
;";

#######Validation for sms
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_uniq_called_sms table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion sms records to solo_uniq_called_sms table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_uniq_called_sms\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_uniq_called_sms where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  
	  $check_sql="
	  select count(9) cnt 
	  from (
	  select substr(try_cast(tbl_dt as varchar),1,6) month,tbl_dt,'sms' event_type,
        case 
        when call_type_enrich = 0 then 'incoming'  
        when call_type_enrich = 1 then 'outgoing' 
        else 'unknown' end event_type_desc,
       chargepartynumber,calledcallingnumber
     from flare_8.cs5_ccn_sms_ma a 
     where tbl_dt = $proc_date 
     group by tbl_dt,chargepartynumber,calledcallingnumber,
     case when call_type_enrich = 0 then 'incoming' when call_type_enrich = 1 then 'outgoing' else 'unknown' end 
	 )
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

print "\nMerging SMS records to Voice records into solo_uniq_called table\n";

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called
select *
from ng_ops_support.solo_uniq_called_sms
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



print "\nPopulating table solo_uniq_called_tmp\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_tmp 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_tmp
select 
month,
tbl_dt,
try_cast(chargepartynumber as bigint) msisdn_key,
calledcallingnumber Unique_Num_contacted_all,
case when event_type = 'call' and event_type_desc = 'outgoing' then calledcallingnumber end  Unique_Num_contacted_outbound_calls,
case when event_type = 'call' and event_type_desc = 'incoming' then calledcallingnumber end  Unique_Num_contacted_incoming_calls,
case when event_type = 'sms' then calledcallingnumber end  Unique_Num_SMS_all,
case when event_type = 'sms' and event_type_desc = 'outgoing' then calledcallingnumber end  Unique_Num_contacted_outbound_SMS,
case when event_type = 'sms' and event_type_desc = 'incoming' then calledcallingnumber end  Unique_Num_contacted_inbound_SMS,
case when event_type = 'call' then counts end  call_counts,
case when event_type = 'sms' then counts end  sms_counts
from ng_ops_support.solo_uniq_called 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



#####################
#####################
print "\nPopulating table solo_uniq_called_tmp2\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_tmp2 where tbl_dt = $proc_date
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_tmp2
select month,msisdn_key,
count(distinct unique_num_contacted_all) unique_num_contacted_all,
count(distinct unique_num_contacted_outbound_calls) unique_num_contacted_outbound_calls,
count(distinct unique_num_contacted_incoming_calls) unique_num_contacted_incoming_calls,
count(distinct unique_num_sms_all) unique_num_sms_all,
count(distinct unique_num_contacted_outbound_sms) unique_num_contacted_outbound_sms,
count(distinct unique_num_contacted_inbound_sms) unique_num_contacted_inbound_sms,
tbl_dt
from ng_ops_support.solo_uniq_called_tmp
group by month,tbl_dt,msisdn_key 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

#########VOICE
print "\nPopulating table solo_uniq_called_voice_tmp2\n";

$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp2 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp2
select month,msisdn_key,unique_num_contacted_all called_num,call_counts,tbl_dt
from ng_ops_support.solo_uniq_called_tmp
where month = substr(try_cast($start_date as varchar),1,6) 
and call_counts is not null 
and unique_num_contacted_outbound_calls is not null 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nPopulating table solo_uniq_called_voice_tmp2a\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_voice_tmp2a where tbl_dt =  $proc_date
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_voice_tmp2a
select month,msisdn_key,called_num,call_counts,tbl_dt 
from(
select month,msisdn_key,called_num,call_counts,tbl_dt,
row_number() over (partition by msisdn_key order by call_counts desc) ranking
from ng_ops_support.solo_uniq_called_voice_tmp2
where tbl_dt = $proc_date
) 
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


#############SMS
print "\nPopulating table solo_uniq_called_sms_tmp2\n";
$sql_stmt = "
delete from ng_ops_support.solo_uniq_called_sms_tmp2 where tbl_dt = $proc_date
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_uniq_called_sms_tmp2
select month,msisdn_key,unique_num_contacted_all called_num,sum(sms_counts) sms_counts,tbl_dt
from ng_ops_support.solo_uniq_called_tmp
where tbl_dt = $proc_date
and sms_counts is not null
and unique_num_contacted_outbound_sms is not null 
group by month,msisdn_key,unique_num_contacted_all,tbl_dt
;";
$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


######################
######################


print "\nPROCESSING table solo_recharge_interv_tmp \n";

####Recharge Part 
$sql_stmt_del = "
delete from ng_ops_support.solo_recharge_interv_tmp where tbl_dt = $proc_date
;";

$sql_stmt = "
insert into ng_ops_support.solo_recharge_interv_tmp
select substr(try_cast(tbl_dt as varchar),1,6) months,try_cast('234'||accountnumber as bigint) msisdn,tbl_dt
from flare_8.cs5_air_refill_ma
where tbl_dt = $proc_date
group by substr(try_cast(tbl_dt as varchar),1,6),tbl_dt,try_cast('234'||accountnumber as bigint)
;";

#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_recharge_interv_tmp table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_recharge_interv_tmp table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_recharge_interv_tmp\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_recharge_interv_tmp where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct accountnumber) cnt from flare_8.cs5_air_refill_ma where tbl_dt = $proc_date";
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

######################
######################


##### AVERAGE BALANCE BEFORE RECHARGE

$sql_stmt_del = "
delete from ng_ops_support.xtratime_balance_tmp where tbl_dt = $proc_date
;";

$sql_stmt = "
insert into ng_ops_support.xtratime_balance_tmp 
select substr(try_cast(tbl_dt as varchar),1,6) month,
msisdn_key,
round(avg(case when adjustmentaction = '1' then abs(try_cast(balancebefore as double)) end),2) balancebefore_rch,
round(sum(case when adjustmentaction = '1' then abs(try_cast(balancebefore as double)) end),2) balancebefore_rch_sum,
sum(case when adjustmentaction = '1' and abs(try_cast(balancebefore as double)) > 0 then 1 end) balancebefore_rch_counts,
sum(case when originnodeid ='ACS' and adjustmentaction = '2' and serviceclassid = '243' and transactioncode in ('Xtratime') then 1 else 0 end) Xtratime_counts,
sum(case when originnodeid ='ACS' and adjustmentaction = '2' and serviceclassid = '243' and transactioncode in ('Xtrabyte') then 1 else 0 end) Xtrabyte_counts,
sum(case when originnodeid ='ACS' and adjustmentaction = '2' and serviceclassid = '243' and  transactioncode in ('Xtratime') then try_cast(adjustmentamount as double) else 0 end) Xtratime_loan,
sum(case when originnodeid ='ACS' and adjustmentaction = '2' and serviceclassid = '243' and  transactioncode in ('Xtratime') then try_cast(adjustmentamount as double) else 0 end) Xtrabyte_loan,
tbl_dt
from flare_8.cs5_sdp_acc_adj_ma a 
where a.tbl_dt = $proc_date
and a.adjustmentaction in ('1','2')
group by substr(try_cast(tbl_dt as varchar),1,6),tbl_dt,msisdn_key
;";

#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from xtratime_balance_tmp table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to xtratime_balance_tmp table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table xtratime_balance_tmp\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.xtratime_balance_tmp where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct accountnumber) cnt from flare_8.cs5_sdp_acc_adj_ma where tbl_dt = $proc_date and adjustmentaction in ('1','2')";
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


######################
######################

###########5b5 Usage 


$sql_stmt_del = "
delete from ng_ops_support.solo_data_kpi_5b5_usg_tmp
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5_usg_tmp 
select 
try_cast(substr(try_cast(tbl_dt as varchar),1,6) as int)  months,
tbl_dt,
msisdn_key,
data_local_da_bonus_kb,
data_roam_da_bonus_kb,
data_kb,
case when data_kb/1024 < 2 then 1 else 0 end as GSM_DAYS_BUND_LESS_2MB
from nigeria.segment5b5_usg a
where tbl_dt = $proc_date
and aggr = 'daily' 
;";


#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_kpi_5b5_usg_tmp table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_kpi_5b5_usg_tmp table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_kpi_5b5_usg_tmp\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_data_kpi_5b5_usg_tmp where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(9) cnt from nigeria.segment5b5_usg where tbl_dt = $proc_date and aggr = 'daily' ";
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
delete from ng_ops_support.solo_data_kpi_5b5_usg_tmp2 where tbl_dt = $proc_date
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5_usg_tmp2
select 
months,
msisdn_key,
sum(data_local_da_bonus_kb) data_local_da_bonus_kb,
sum(data_roam_da_bonus_kb) data_roam_da_bonus_kb,
sum(data_kb) data_kb,
sum(GSM_DAYS_BUND_LESS_2MB) GSM_DAYS_BUND_LESS_2MB,
tbl_dt
from ng_ops_support.solo_data_kpi_5b5_usg_tmp a
where tbl_dt = $proc_date
group by months,tbl_dt,msisdn_key
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


################



#############
###########5b5 Revenue  
$sql_stmt_del = "
delete from ng_ops_support.solo_data_kpi_5b5_rev_tmp 
;";

$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5_rev_tmp 
select 
try_cast(substr(try_cast(tbl_dt as varchar),1,6) as int)  months,
msisdn_key,
spend_tot,
xtratime_loan,
tbl_dt
from nigeria.segment5b5_rev a
where tbl_dt = $proc_date
and aggr = 'daily'
;";

#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_kpi_5b5_rev_tmp table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_kpi_5b5_rev_tmp table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_kpi_5b5_rev_tmp\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_data_kpi_5b5_rev_tmp where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(9) cnt from nigeria.segment5b5_rev where tbl_dt = $proc_date and aggr = 'daily' ";
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
delete from ng_ops_support.solo_data_kpi_5b5_rev_tmp2 where tbl_dt = $proc_date
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_5b5_rev_tmp2 
select 
months,
msisdn_key,
case when sum(spend_tot) > 0 then 1 else 0 end  gsm_active_days_qty,
max(xtratime_loan) xtratime_loan,
tbl_dt 
from ng_ops_support.solo_data_kpi_5b5_rev_tmp a 
where tbl_dt = $proc_date
group by months,tbl_dt,msisdn_key
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;


#####
#####SPRINT2

#####CRBT 

$sql_stmt = "
delete from ng_ops_support.solo_data_crbt_subscription_tmp where tbl_dt = $proc_date
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into ng_ops_support.solo_data_crbt_subscription_tmp 
select month,msisdn_key,count(msisdn_key) Number_of_ring_back_tones_subscriptions,sum(rev)  Vaue_of_ring_back_tones_subscriptions,try_cast(tbl_dt as int) tbl_dt
from(
select substr(try_cast(tbl_dt as varchar),1,6) month,tbl_dt,msisdn_key,sub_fee rev
from nigeria.hsdp_sumd a---crbt subscription
where partner_id = '2340110008059'
and tbl_dt = $proc_date 
union all
select substr(try_cast(date_key as varchar),1,6) month,date_key tbl_dt, msisdn_key ,amount rev
from  nigeria.daas_daily_usage_by_msisdn c --crbt download                                                                                                           
where date_key = $proc_date                                                                                                                   
and upper(event_type)  in ( 'BOX DOWNLOAD','TUNE DOWNLOAD' )
) 
group by month,tbl_dt,msisdn_key
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;



######VAS Counts

$sql_stmt_del = "
delete from ng_ops_support.solo_data_kpi_subscriptions_tmp where tbl_dt = $proc_date
;";


$sql_stmt = "
insert into ng_ops_support.solo_data_kpi_subscriptions_tmp 
select try_cast(substr(try_cast(tbl_dt as varchar),1,6) as int)  months,msisdn_key,count(*) Total_subscriptions_count,tbl_dt
from flare_8.vp_cs5_ccn_gprs_ma_dasplit_scap
where tbl_dt = $proc_date 
and originatingservices in ('CIS','HWCRBT','HWSDP')
group by try_cast(substr(try_cast(tbl_dt as varchar),1,6) as int) ,tbl_dt,msisdn_key
;";


#######Validation 
$cnt_exe = 0 ; 
while( $cnt_exe < $max_exe_tries )
{
      
	  print "\nDeletion from solo_data_kpi_subscriptions_tmp table\n";
	  $cmd_del =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt_del;commit;"`;
	  
	  print "\nInsertion records to solo_data_kpi_subscriptions_tmp table\n";
	  $cmd     =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $sql_stmt;commit;"`;

print "\nVaidation of data in table solo_data_kpi_subscriptions_tmp\n";

##Part1 check of Output table  
print "\nCounts of Final table\n";
	  $check_sql="select count(1) cnt from ng_ops_support.solo_data_kpi_subscriptions_tmp where tbl_dt = $proc_date";
	  $rec_cnt =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags daasuser --execute "Start transaction; $check_sql;commit;"`;
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 

##Part1 check of Base table 
print "\nCounts of Source table\n"; 
	  $check_sql="select count(distinct msisdn_key) cnt from flare_8.vp_cs5_ccn_gprs_ma_dasplit_scap where tbl_dt = $proc_date and originatingservices in ('CIS','HWCRBT','HWSDP') ";
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

 
}
print "\nEND OF PROCESSING \n";

}









