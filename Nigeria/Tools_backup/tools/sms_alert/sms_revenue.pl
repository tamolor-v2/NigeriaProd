## Procedure for SMS
#!/usr/bin/env perl
#use strict;
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
my $i = 35;
my $sql_stmt; my $sql_stmt1; my $sql_stmt2; my $sql_stmt3; my $sql_stmt4; my $sql_stmt5; my $sql_stmt6; my $sql_stmt7; my $sql_stmt8; my $sql_stmt9; my $sql_stmt10;
my $sql_stmt11; my $sql_stmt12; my $sql_stmt13; my $sql_stmt14; my $sql_stmt15; my $sql_stmt16; my $sql_stmt17; my $sql_stmt18; my $sql_stmt19;
my $voice1; my $voice2; my $vas1; my $vas2;  my $data1; my $data2;  my $sms1; my $sms2; 
my $mb1; my $mb2; my $riv1; my $riv2;  my $tot1; my $tot2;
my $filename;
my $cmd;
my $dir  = "/mnt/beegfs/tools/sms_alert";
my $test;
my $check_sql;
my $rec_cnt;
my $v_msg ;
my $v_rgs1; my $v_rgs11;
my $v_rgs90;
my $proc_date; my $proc_date2;
my $proc_date_day; my $proc_date_day2;
my $CLASSPATH;
my $rms;
my @nums; my @v_d1; my $v_d1;
my $numt;
my $values = @ARGV;
my $value_cnt=scalar $values;
my $groupid   = $ARGV[0];
my $i   = $ARGV[1];
my $ii   = $ARGV[2];


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
 
if ($pc gt 3 ) { print "\nSMS job is currently running\n"; exit; }  ## exit if sample job is already running

if ($value_cnt lt 1 ) {
    $groupid = 15;
    $i = 1;
	$ii = 2;
    print "\nRange of days not supplied, default of Team selected and initiated \n"; 
 }


print "\nStarting Run\n";
print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');



$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%Y%m%d') AS INT) date;";
$proc_date   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date =~ s/"//g; 
chomp($proc_date);

$check_sql   = "select CAST(date_format(date_add('day', - ($i+1) , current_date ),'%Y%m%d') AS INT) date;";
$proc_date2   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date2 =~ s/"//g; 
chomp($proc_date2);

$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%d/%m/%Y') AS varchar) date;";
$proc_date_day   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day =~ s/"//g; 
chomp($proc_date_day);

$check_sql   = "select CAST(date_format(date_add('day', - ($i+1) , current_date ),'%d/%m/%Y') AS varchar) date;";
$proc_date_day2   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day2 =~ s/"//g; 
chomp($proc_date_day2);

##select * from nigeria.sms_members where group_id= $start_date

$sql_stmt1 ="select msisdn  from nigeria.sms_members where group_id=".$groupid."";
@nums =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt1"`;


$sql_stmt2 ="SELECT voice FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date."";

$sql_stmt3 ="SELECT voice FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date2."";



$sql_stmt4 ="SELECT sms FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date."";

$sql_stmt5 ="SELECT sms FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date2."";

$sql_stmt6 ="SELECT vas FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date."";

$sql_stmt7 ="SELECT vas FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date2."";


$sql_stmt8 ="SELECT data FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date."";

$sql_stmt9 ="SELECT data FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date2."";


$sql_stmt10 ="SELECT sum(voice+data+vas+sms) FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date."";

$sql_stmt11 ="SELECT sum(voice+data+vas+sms) FROM nigeria.daas_daily_prepaid_revenue_sms
where date_key= ".$proc_date2."";


$voice1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt2"`;
$voice2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt3"`;
$voice1 =~ s/"//g; 
$voice2 =~ s/"//g; 

$sms1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt4"`;
$sms2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt5"`;
$sms1 =~ s/"//g; 
$sms2 =~ s/"//g; 


$vas1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt6"`;
$vas2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt7"`;
$vas1 =~ s/"//g; 
$vas2 =~ s/"//g;

$data1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt8"`;
$data2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt9"`;
$data1 =~ s/"//g;  
$data2 =~ s/"//g;

$tot1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt10"`;
$tot2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt11"`;
$tot1 =~ s/"//g; 
$tot2 =~ s/"//g;


#@nums = (2347062029498,2347030035237,2349031699027,2348108652827,2348032002722,2347062029762,2348039480583,2348181000016,2349039516051,2347067113221,2348032002141,2348032008175,2347068889651,2347062029700,2348095078301,2348069183948,2347062029888,2349038361230,2348147243462,2348142255283,2348032009259,2348135292397,2348032000244,2348032008647,2348032008446,2348137265678,2348032003662,2348032000315,2348143474361);

@nums = (2347062029498,2348137265678);   #,2348032004503   Dime 2348032008446

#@nums = (2347062029498,2347030035237,2349031699027,2348108652827,2348032002722,2347062029762,2348039480583,2348181000016,2349039516051,2347067113221,2348032002141,2348032008175,2347068889651,2347062029700,2348095078301,2348069183948,2347062029888,2349038361230,2348147243462,2348142255283,2348032009259,2348135292397,2348032000244,2348032008647,2348032008446,2348137265678,2348032003662,2348032000315,2348143474361);


$voice1= $voice1*1.0; #$voice1  = (substr $voice1, -9, 3).M; 
$voice2= $voice2*1.0; #$voice2  = (substr $voice2, -9, 3).M; 
$sms1= $sms1*1.0; #$sms1  = (substr $sms1, -9, 3).M; 
$sms2= $sms2*1.0; #$sms2  = (substr $sms2, -9, 3).M; 
$vas1 = $vas1*1.0; #$vas1  = (substr $vas1, -9, 3).M; 
$vas2 = $vas2*1.0; #$vas2  = (substr $vas2, -9, 3).M; 
$data1= $data1*1.0; #$data1  = (substr $data1, -9, 3).M; 
$data2= $data2*1.0; #$data2  = (substr $data2, -9, 3).M; 
$tot1 = $tot1*1.0;
$tot2 = $tot2*1.0;

$voice1 =~ s/[^a-zA-Z0-9]*//g;
$voice2 =~ s/[^a-zA-Z0-9]*//g;
$sms1  =~ s/[^a-zA-Z0-9]*//g;
$sms2  =~ s/[^a-zA-Z0-9]*//g;
$vas1 =~ s/[^a-zA-Z0-9]*//g;
$vas2 =~ s/[^a-zA-Z0-9]*//g;
$data1  =~ s/[^a-zA-Z0-9]*//g;
$data2  =~ s/[^a-zA-Z0-9]*//g;
$tot1  =~ s/[^a-zA-Z0-9]*//g;
$tot2  =~ s/[^a-zA-Z0-9]*//g;
$voice1 = function_split($voice1);
$voice2 = function_split($voice2);
$sms1 = function_split($sms1);
$sms2 = function_split($sms2);
$vas1 = function_split($vas1);
$vas2 = function_split($vas2);
$data1 = function_split($data1);
$data2 = function_split($data2);
$tot1 = function_split($tot1);
$tot2 = function_split($tot2);

$v_msg="Prepaid Revenue $proc_date_day vs $proc_date_day2: \nVoice: $voice1 / $voice2 \nSMS: $sms1 / $sms2 \nVAS: $vas1 / $vas2 \nDATA: $data1 / $data2 \n(Total: $tot1 / $tot2 )";

#\rMIDDLE BELT:  $mb1  $mb2


     
foreach $numt (@nums) {
$numt =~ s/"//g;
$numt=~ s/[^a-zA-Z0-9]*//g;
print "Sending for: $numt\n";
print "message: $v_msg\n";
#system(`java -classpath "$CLASSPATH:lib/smppapi.jar:lib/commons-logging.jar:lib/log4j-1.2.5.jar:lib/edwsmpp.jar:lib/ojdbc14_g.jar" mtnn.smpp.edw.SyncTransmitter 10.206.140.172 10000 NG_DaaS "7ygv&YGV" "" "$numt" "$v_msg" "DAAS" 5 9`);
  
  sleep 1;
}



}
