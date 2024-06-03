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
my $filename;  my $v_dch;  my $v_mdch; my $area; my $v_rgs2;
my $cmd;
my $dir  = "/mnt/beegfs/tools/sms_alert";
my $test;
my $check_sql;
my $rec_cnt;
my $v_msg ;
my $v_rgs1; my $v_rgs11;
my $v_rgs90;
my $proc_date;
my $proc_date_day; my $proc_date_day2; my $proc_date_first;
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
chomp($proc_date_day2);

##select * from nigeria.sms_members where group_id= $start_date

$sql_stmt1 ="select msisdn  from nigeria.sms_members where group_id=".$groupid."";
#@nums =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt1"`;

$sql_stmt2 ="select rgs1  from nigeria.daily_rgs where tbl_dt=".$proc_date."";
$v_rgs1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt2"`;
$v_rgs1 =~ s/"//g; 


$sql_stmt3 ="select rgs90  from nigeria.daily_rgs where tbl_dt=".$proc_date."";
$v_rgs90 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt3"`;
$v_rgs90 =~ s/"//g; 

$sql_stmt4 ="select kpi000004 from kpi.kpi_val where kpi_id='KPI-000004' and tbl_dt=".$proc_date."";
$v_dch =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt4"`;
$v_dch =~ s/"//g; 

$sql_stmt5 ="select sum(kpi000004) from kpi.kpi_val where kpi_id='KPI-000004' and tbl_dt >= ".$proc_date_first." and tbl_dt <= ".$proc_date."";
$v_mdch =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt5"`;
$v_mdch =~ s/"//g; 

$sql_stmt6 ="select ceiling(cast(refilldivisionamount____1 as decimal)) from flare_8.CS5_AIR_REFILL_DA where tbl_dt=20190102";
$v_dc =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt5"`;
$v_dc =~ s/"//g; 


@nums = (2347062029498);  #Yetunde ,2348032008647 ,2348137265678

#$v_msg ='RGS Figures as at '.$proc_date_day.chr(10).
#           rpad('RGS90 :',12,' ').TO_CHAR($v_rgs90,'99,999,999').chr(10). 
#           rpad('RGS01 :',12,' ').TO_CHAR($v_rgs1,'99,999,999').chr(10),;

$v_rgs1 =~ s/[^a-zA-Z0-9]*//g;
$v_rgs90 =~ s/[^a-zA-Z0-9]*//g;
$v_dch  =~ s/[^a-zA-Z0-9]*//g;
$v_mdch  =~ s/[^a-zA-Z0-9]*//g;
$v_dc  =~ s/[^0-9]*//g;
$v_rgs1 = function_split($v_rgs1);
$v_rgs90 = function_split($v_rgs90);
$v_dch = function_split($v_dch);
$v_mdch = function_split($v_mdch);
$v_dc = function_split($v_dc);

$v_msg="RGS Figures as at $proc_date_day\nRGS90: $v_rgs90 \nRGS1: $v_rgs1 \nDLY RGS CONN: $v_dc\nDLY KIT ACTV:\nMTD NET CONN:\nDLY CHURN: $v_dch\nMtdGrChurn: $v_mdch";

#$v_msg="RGS Figures as at $proc_date_day\nRGS90: $v_rgs90 RGS1: $v_rgs1 later:$v_rgs2 \nDLY RGS CONN:\nDLY KIT ACTV:\nMTD NET CONN:\nDLY CHURN: $v_dch MtdGrChurn: $v_mdch";

#$v_msg="Recharge by Region $proc_date_day vs $proc_date_day2: \nCoreEast: $core_east1 / $core_east2 \nCoreWest:$core_west1 / $core_west2 \nFarEast: $far_east1 / $far_east2 \nFarNorth: $far_north1 / $far_north2 \nLagos/Ogun: $lag1 / $lag2 \nMbelt: $mb1 / $mb2 \nUnk: $unk1 / $unk2 \n(Total: $tot1 / $tot2 )";
       
foreach $numt (@nums) {
#$numt =~ s/"//g; 
print "Team msisdn: $numt\n";
print "message: $v_msg\n";
#system(`java -classpath "$CLASSPATH:lib/smppapi.jar:lib/commons-logging.jar:lib/log4j-1.2.5.jar:lib/edwsmpp.jar:lib/ojdbc14_g.jar" mtnn.smpp.edw.SyncTransmitter 10.206.140.172 10000 NG_DaaS "7ygv&YGV" "" "$numt" "$v_msg" "DAAS" 5 9`);
 
  sleep 3;
}



}
