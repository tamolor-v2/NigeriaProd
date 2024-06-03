## Procedure for SMS
#!/usr/bin/env perl
use strict;
use FileHandle;
use Getopt::Long;
use IO::Handle;
use IO::Socket;

#
my @months      = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my @months      = qw(01 02 03 04 05 06 07 08 09 10 11 12);
my @weekDays    = qw(Sun Mon Tue Wed Thu Fri Sat Sun);
my @dates;

 

my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
my $year = 1900 + $yearOffset;
my $theTime = "$hour:$minute:$second, $weekDays[$dayOfWeek] $months[$month] $dayOfMonth, $year";
my $procDate;
my $epoc = time();
my $sql_stmt; my $sql_stmt1; my $sql_stmt2; my $sql_stmt3;  my $sql_stmt4;  my $sql_stmt5;  my $sql_stmt6; my $sql_stmt7;
my $filename;  my $v_dch;  my $v_mdch; my $area; my $v_rgs2; my $v_dc; my $v_mdc;
my $cmd;
my $dir  = "/mnt/beegfs/tools/sms_alert";
my $test;
my $check_sql;
my $rec_cnt;
my $v_msg ;
my $v_rgs1; my $v_rgs11;
my $v_rgs90;
my $proc_date; my $proc_date2;
my $proc_date_day; my $proc_date_day2; my $proc_date_first; my $proc_date_last;
my $CLASSPATH;
my $rms;
my $nums;
my @nums;
my $numt;
my $values = @ARGV;
my $value_cnt=scalar $values;
my $groupid   = $ARGV[0];
my $i   = $ARGV[1];


  
{

system('clear');

sub function_split {
  my ($r1) = @_;
  my $check_sql = "select 
trim(substr('".$r1."',1,mod(length('".$r1."'),3))||case when length('".$r1."')>1+mod(length('".$r1."'),3) and mod(length('".$r1."'),3)>0 then ',' else '' end||
substr(substr('".$r1."',1+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>4+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',4+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>7+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',7+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>10+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',10+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>13+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',13+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>16+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',16+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>19+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',19+mod(length('".$r1."'),3)),1,3)||case when length('".$r1."')>22+mod(length('".$r1."'),3) then ',' else '' end||
substr(substr('".$r1."',22+mod(length('".$r1."'),3)),1,3)) formatted_nr;";
$area = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$area =~ s/"//g; 
chomp($area); 
  return $area;
  }


$cmd =  `cd $dir`;
my $pc = `ps -ef |grep sms_alert.pl |wc -l`;
$pc =~ s/^\s+//;  ## strip leading and trailing spaces
chomp($pc);      
 
if ($pc gt 5 ) { print "\nSMS job is currently running\n"; exit; }  ## exit if sample job is already running

if ($value_cnt lt 1 ) {
    $groupid = 15;
    $i = 1;
    print "\nRange of days not supplied, default of D-7 days initiated \n"; 
 }


print "\nStarting Trend\n";
print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');



$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%Y%m%d') AS INT) date;";
$proc_date   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date =~ s/"//g; 
chomp($proc_date);

$check_sql   = "select CAST(date_format(date_add('day', - ($i+1) , current_date ),'%Y%m%d') AS INT) date;";
$proc_date2   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date2 =~ s/"//g; 
chomp($proc_date);

$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%d %M,%Y') AS varchar) date;";
$proc_date_day   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day =~ s/"//g; 
chomp($proc_date_day);

$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%Y-%m-%d') AS varchar) date;";
$proc_date_day2   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day2 =~ s/"//g; 
chomp($proc_date_day2);

$check_sql   = "select date_trunc('month', DATE '".$proc_date_day2."');";
$proc_date_first   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_first =~ s/"//g; 
$proc_date_first =~ s/-//g; 
chomp($proc_date_first);


$check_sql   = "select date_add('day', -1, date_trunc('month', DATE '".$proc_date_day2."'));";
$proc_date_last   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_last =~ s/"//g; 
$proc_date_last =~ s/-//g; 
chomp($proc_date_last);

##select * from nigeria.sms_members where group_id= $start_date

$sql_stmt1 ="select msisdn  from nigeria.sms_members where group_id=".$groupid."";
#$sql_stmt1 ="select msisdn  from nigeria.sms_members where msisdn in (2347062029498,2348137265678)";
@nums =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt1"`;

$sql_stmt2 ="select rgs1  from nigeria.daily_metrics where tbl_dt=".$proc_date."";
$v_rgs1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt2"`;
$v_rgs1 =~ s/"//g; 


$sql_stmt3 ="select rgs90  from nigeria.daily_metrics where tbl_dt=".$proc_date."";
$v_rgs90 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt3"`;
$v_rgs90 =~ s/"//g; 

$sql_stmt4 ="select gross_churn  from nigeria.daily_metrics where tbl_dt=".$proc_date."";
$v_dch =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt4"`;
$v_dch =~ s/"//g; 

$sql_stmt5 ="select mtd_gross_churn  from nigeria.daily_metrics where tbl_dt=".$proc_date."";
$v_mdch =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt5"`;
$v_mdch =~ s/"//g; 

# $sql_stmt6 ="select count(1) from (
# select msisdn_key
# from
     # flare_8.customersubject
# where aggr='daily' and dola between 0 and 89 and tbl_dt=".$proc_date." 
      # and msisdn_key not in (
                # select msisdn_key
                # from
                    # flare_8.customersubject
                # where aggr='daily' and dola between 0 and 89 and tbl_dt=".$proc_date2."
                # )
      # and msisdn_key in (select distinct msisdn_key from nigeria.daily_activation_base where tbl_dt between try_cast(date_format(current_date + interval '-91' day,'%Y%m%d') as INTEGER)
# and try_cast(date_format(current_date + interval '-1' day,'%Y%m%d') as INTEGER) )
      # )";
$sql_stmt6 ="select gross_conn  from nigeria.daily_metrics where tbl_dt=".$proc_date."";	  
$v_dc =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt6"`;
$v_dc =~ s/"//g; 

$sql_stmt7 ="select mtd_net_conn  from nigeria.daily_metrics where tbl_dt=".$proc_date."";	
$v_mdc =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt7"`;
#$v_mdc=315393; #276881;
$v_mdc =~ s/"//g; 

#@nums = (2347062029498,2347030035237,2349031699027,2348108652827,2348032002722,2347062029762,2348039480583,2348181000016,2349039516051,2347067113221,2348032002141,2348032008175,2347068889651,2347062029700,2348095078301,2348069183948,2347062029888,2349038361230,2348147243462,2348142255283,2348032009259,2348135292397,2348032000244,2348032008446,2348032008647,2348137265678,2348032003662,2348032000315,2348143474361);


#@nums = (2347062029498,2348137265678) ;#,2348137265678);  #Yetunde:2348032008647 ,2348137265678  


#######@nums = (2348032008446,2348032008647); #Dime, Yetunde

#$v_msg ='RGS Figures as at '.$proc_date_day.chr(10).
#           rpad('RGS90 :',12,' ').TO_CHAR($v_rgs90,'99,999,999').chr(10). 
#           rpad('RGS01 :',12,' ').TO_CHAR($v_rgs1,'99,999,999').chr(10),;
#$v_mdc = $v_rgs1-$v_mdc;
$v_rgs1 =~ s/[^a-zA-Z0-9]*//g;
$v_rgs90 =~ s/[^a-zA-Z0-9]*//g;
$v_dch  =~ s/[^a-zA-Z0-9]*//g;
$v_mdch  =~ s/[^a-zA-Z0-9]*//g;
$v_dc  =~ s/[^a-zA-Z0-9]*//g;
$v_mdc  =~ s/[^a-zA-Z0-9]*//g;
$v_rgs1 = function_split($v_rgs1);
$v_rgs90 = function_split($v_rgs90);
$v_dch = function_split($v_dch);
$v_mdch = function_split($v_mdch); 
$v_dc = function_split($v_dc);
$v_mdc = function_split($v_mdc);



$v_msg="RGS Figures as at $proc_date_day\nRGS90: $v_rgs90 \nRGS1: $v_rgs1 \nDLY RGS CONN: $v_dc\nDLY KIT ACTV:\nMTD NET CONN: $v_mdc\nDLY CHURN: $v_dch\nMtdGrChurn: $v_mdch";

#$v_msg="RGS Figures as at $proc_date_day\nRGS90: $v_rgs90 RGS1: $v_rgs1 later:$v_rgs2 \nDLY RGS CONN:\nDLY KIT ACTV:\nMTD NET CONN:\nDLY CHURN: $v_dch MtdGrChurn: $v_mdch";

#$v_msg="Recharge by Region $proc_date_day vs $proc_date_day2: \nCoreEast: $core_east1 / $core_east2 \nCoreWest:$core_west1 / $core_west2 \nFarEast: $far_east1 / $far_east2 \nFarNorth: $far_north1 / $far_north2 \nLagos/Ogun: $lag1 / $lag2 \nMbelt: $mb1 / $mb2 \nUnk: $unk1 / $unk2 \n(Total: $tot1 / $tot2 )";
       
foreach $numt (@nums) {
$numt =~ s/"//g;
$numt=~ s/[^a-zA-Z0-9]*//g;
print "Team msisdn: $numt\n";
print "message: $v_msg\n";
system(`java -classpath "$CLASSPATH:lib/smppapi.jar:lib/commons-logging.jar:lib/log4j-1.2.5.jar:lib/edwsmpp.jar:lib/ojdbc14_g.jar" mtnn.smpp.edw.SyncTransmitter 10.206.140.172 10000 NG_DaaS "7ygv&YGV" "" "$numt" "$v_msg" "DAAS" 5 9`);
 
  sleep 3;
}



}
