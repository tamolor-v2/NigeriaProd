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
my $core_east1; my $core_east2; my $core_west1; my $core_west2;  my $far_east1; my $far_east2;  my $far_north1; my $far_north2;  my $lag1; my $lag2; my $unk1; my $unk2;
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
my $nums;
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
    print "\nRange of days not supplied, default of D-7 days initiated \n"; 
 }


print "\nStarting Run\n";
print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');



$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%Y%m%d') AS INT) date;";
$proc_date   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date =~ s/"//g; 
chomp($proc_date);

$check_sql   = "select CAST(date_format(date_add('day', -($i+1) , current_date ),'%Y%m%d') AS INT) date;";
$proc_date2   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date2 =~ s/"//g; 
chomp($proc_date2);

$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%d/%m/%Y') AS varchar) date;";
$proc_date_day   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day =~ s/"//g; 
chomp($proc_date_day);

$check_sql   = "select CAST(date_format(date_add('day',-($i+1) , current_date ),'%d/%m/%Y') AS varchar) date;";
$proc_date_day2   = `presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day2 =~ s/"//g; 
chomp($proc_date_day2);

##select * from nigeria.sms_members where group_id= $start_date

$sql_stmt1 ="select msisdn  from nigeria.sms_members where group_id=".$groupid."";
@nums =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt1"`;


$sql_stmt2 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='CORE EAST'
group   by tbl_dt,region";

$sql_stmt3 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='CORE EAST'
group   by tbl_dt,region";



$sql_stmt4 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='CORE WEST'";

$sql_stmt5 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='CORE WEST'";

$sql_stmt6 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='FAR EAST'";

$sql_stmt7 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='FAR EAST'";


$sql_stmt8 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='FAR NORTH'";

$sql_stmt9 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='FAR NORTH'";


$sql_stmt10 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='LAGOS/OGUN'";

$sql_stmt11 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='LAGOS/OGUN'";


$sql_stmt12 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='MIDDLE BELT'";

$sql_stmt13 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='MIDDLE BELT'";


$sql_stmt14 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='UNKNOWN'";

$sql_stmt15 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='UNKNOWN'";


$sql_stmt16 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."
and region='RIVERINE'";

$sql_stmt17 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."
and region='RIVERINE'";

$sql_stmt18 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date."";

$sql_stmt19 ="select  ceiling(sum(amount)) amount
from    nigeria.caas_rechargesbyregion
where   tbl_dt = ".$proc_date2."";


$core_east1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt2"`;
$core_east2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt3"`;
$core_east1 =~ s/"//g; 
$core_east2 =~ s/"//g; 

$core_west1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt4"`;
$core_west2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt5"`;
$core_west1 =~ s/"//g; 
$core_west2 =~ s/"//g; 


$far_east1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt6"`;
$far_east2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt7"`;
$far_east1 =~ s/"//g; 
$far_east2 =~ s/"//g;

$far_north1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt8"`;
$far_north2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt9"`;
$far_north1 =~ s/"//g; 
$far_north2 =~ s/"//g;

$lag1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt10"`;
$lag2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt11"`;
$lag1 =~ s/"//g; 
$lag2 =~ s/"//g;

$mb1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt12"`;
$mb2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt13"`;
$mb1 =~ s/"//g; 
$mb2 =~ s/"//g;

$unk1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt14"`;
$unk2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt15"`;
$unk1 =~ s/"//g; 
$unk2 =~ s/"//g;

$riv1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt16"`;
$riv2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt17"`;
$riv1 =~ s/"//g; 
$riv2 =~ s/"//g;

$tot1 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt18"`;
$tot2 =`presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt19"`;
$tot1 =~ s/"//g; 
$tot2 =~ s/"//g;

#@nums = (2347062029498,2347030035237,2349031699027,2348108652827,2348032002722,2347062029762,2348039480583,2348181000016,2349039516051,2347067113221,2348032002141,2348032008175,2347068889651,2347062029700,2348095078301,2348069183948,2347062029888,2349038361230,2348147243462,2348142255283,2348032009259,2348135292397,2348032000244,2348032008446,2348032008647,2348137265678,2348032003662,2348032000315,2348143474361,2348032006779); 

##2348032008446,2348032008647 #Dime, Yetunde


#@nums = (2347062029498,2348137265678) ;#,2348137265678);  #Yetunde:2348032008647 ,2348137265678 






$core_east1= $core_east1*1.0; #$core_east1  = (substr $core_east1, -9, 3).M; 
$core_east2= $core_east2*1.0; #$core_east2  = (substr $core_east2, -9, 3).M; 
$core_west1= $core_west1*1.0; #$core_west1  = (substr $core_west1, -9, 3).M; 
$core_west2= $core_west2*1.0; #$core_west2  = (substr $core_west2, -9, 3).M; 
$far_east1 = $far_east1*1.0; #$far_east1  = (substr $far_east1, -9, 3).M; 
$far_east2 = $far_east2*1.0; #$far_east2  = (substr $far_east2, -9, 3).M; 
$far_north1= $far_north1*1.0; #$far_north1  = (substr $far_north1, -9, 3).M; 
$far_north2= $far_north2*1.0; #$far_north2  = (substr $far_north2, -9, 3).M; 
$lag1 = $lag1*1.0; #$lag1  = (substr $lag1, -9, 3).M;
$lag2 = $lag2*1.0; #$lag2  = (substr $lag2, -9, 3).M;
$mb1 = $mb1*1.0; #$mb1  = (substr $mb1, -9, 3).M;
$mb2 = $mb2*1.0; #$mb2  = (substr $mb2, -9, 3).M;
$unk1 = $unk1*1.0; #$unk1  = (substr $unk1, -9, 3).M;
$unk2 = $unk2*1.0; #$unk2  = (substr $unk2, -9, 3).M;
$riv1 = $riv1*1.0; #$riv1  = (substr $riv1, -9, 3).M;
$riv2 = $riv2*1.0; #$riv2  = (substr $riv2, -9, 3).M;
$tot1 = $tot1*1.0;
$tot2 = $tot2*1.0;

$v_msg="Recharge by Region $proc_date_day vs $proc_date_day2:\nCoreEast:$core_east1/ $core_east2\nCoreWest:$core_west1/ $core_west2 \nFarEast:$far_east1/ $far_east2 \nFarNorth $far_north1/ $far_north2\nLagos/Ogun:$lag1/ $lag2 \nMbelt:$mb1/ $mb2 \nRiverine:$riv1/ $riv2\nUnk:$unk1/ $unk2\n(Total: $tot1/ $tot2 )";

print " $v_msg\n";

$core_east1= $core_east1*1.0; $core_east1  = (substr $core_east1, -9, 3).M; 
$core_east2= $core_east2*1.0; $core_east2  = (substr $core_east2, -9, 3).M; 
$core_west1= $core_west1*1.0; $core_west1  = (substr $core_west1, -9, 3).M; 
$core_west2= $core_west2*1.0; $core_west2  = (substr $core_west2, -9, 3).M; 
$far_east1 = $far_east1*1.0; $far_east1  = (substr $far_east1, -9, 3).M; 
$far_east2 = $far_east2*1.0; $far_east2  = (substr $far_east2, -9, 3).M; 
$far_north1= $far_north1*1.0; $far_north1  = (substr $far_north1, -9, 3).M; 
$far_north2= $far_north2*1.0; $far_north2  = (substr $far_north2, -9, 3).M; 
$lag1 = $lag1*1.0; $lag1  = (substr $lag1, -9, 3).M;
$lag2 = $lag2*1.0; $lag2  = (substr $lag2, -9, 3).M;
$mb1 = $mb1*1.0; $mb1  = (substr $mb1, -9, 3).M;
$mb2 = $mb2*1.0; $mb2  = (substr $mb2, -9, 3).M;
$unk1 = $unk1*1.0; $unk1  = (substr $unk1, -9, 3).M;
$unk2 = $unk2*1.0; $unk2  = (substr $unk2, -9, 3).M;
$riv1 = $riv1*1.0; $riv1  = (substr $riv1, -9, 3).M;
$riv2 = $riv2*1.0; $riv2  = (substr $riv2, -9, 3).M;
$tot1 = $tot1*1.0;
$tot2 = $tot2*1.0;


$tot1  =~ s/[^a-zA-Z0-9]*//g;
$tot2  =~ s/[^a-zA-Z0-9]*//g;
$tot1 = function_split($tot1);
$tot2 = function_split($tot2);

$v_msg="Recharge by Region $proc_date_day vs $proc_date_day2:\nCoreEast:$core_east1/ $core_east2\nCoreWest:$core_west1/ $core_west2 \nFarEast:$far_east1/ $far_east2 \nFarNorth $far_north1/ $far_north2\nLagos/Ogun:$lag1/ $lag2 \nMbelt:$mb1/ $mb2 \nRiverine:$riv1/ $riv2\nUnk:$unk1/ $unk2\n(Total: $tot1/ $tot2 )";


#\rMIDDLE BELT:  $mb1  $mb2


     
foreach $numt (@nums) {
$numt =~ s/"//g;
$numt=~ s/[^a-zA-Z0-9]*//g;
print "Sending for: $numt\n";
print "message: $v_msg\n";
system(`java -classpath "$CLASSPATH:lib/smppapi.jar:lib/commons-logging.jar:lib/log4j-1.2.5.jar:lib/edwsmpp.jar:lib/ojdbc14_g.jar" mtnn.smpp.edw.SyncTransmitter 10.206.140.172 10000 NG_DaaS "7ygv&YGV" "" "$numt" "$v_msg" "DAAS" 5 9`);
  
  sleep 1;
}



}
