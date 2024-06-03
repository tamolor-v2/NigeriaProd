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
my $i = 35;
my $sql_stmt; my $sql_stmt1; my $sql_stmt2; my $sql_stmt3; my $sql_stmt4; my $sql_stmt5; my $sql_stmt6; my $sql_stmt7; my $sql_stmt8; my $sql_stmt9; my $sql_stmt10;
my $filename;
my $cmd;
my $dir  = "/home/wrasheed/scripts";
my $test;
my $check_sql;
my $rec_cnt;
my $i ;
my $proc_date   = $ARGV[0];
my $proc_date_day;
my $rms;
my $values = @ARGV;
my $value_cnt=scalar $values;

## Procedure to generate daily AOD Daily summary

{

system('clear');


$cmd =  `cd $dir`;
my $pc = `ps -ef |grep msc_ccn_trend.pl |wc -l`;
$pc =~ s/^\s+//;  ## strip leading and trailing spaces
chomp($pc);      

if ($pc gt 3 ) { print "\nDaily AOD job is currently running\n"; exit; }  ## exit if sample job is already running


print "\nStarting Trend\n";
print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');



for ($i=0; $i <=$value_cnt-1; $i++) 
{

$check_sql   = "select CAST(date_format(cast('".$ARGV[$i]."' as date),'%Y%m%d') AS varchar) date;";
$proc_date   = `/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date =~ s/"//g; 
chomp($proc_date);


$check_sql   = "select CAST(date_format(cast('".$ARGV[$i]."' as date),'%Y%m%d %a') AS varchar) date;";
$proc_date_day   = `/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`; 
$proc_date_day =~ s/"//g; 
chomp($proc_date_day);

##print "\n".$proc_date."\n".$proc_date_day."\n" ;

## Starting Voice MA
$sql_stmt1 ="delete from nigeria.tmp_sdp_cdr_trend1";
$sql_stmt2 ="delete from nigeria.tmp_sdp_cdr_trend2";
$sql_stmt3 ="delete from nigeria.tmp_sdp_cdr_trend3";
$sql_stmt4 ="delete from nigeria.tmp_sdp_cdr_trend4";
$sql_stmt5 ="delete from nigeria.tmp_sdp_cdr_trend5";

print "Temp table successfully deleted for ".$proc_date."\n";

$cmd =`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt1"`;
	`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt2"`;
	`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt3"`;
	`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt4"`;
	`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt5"`;


	print "\nAll temp tables cleaned for a new run\n";
$sql_stmt1 = "insert into nigeria.tmp_sdp_cdr_trend1 
select  cast(split(file_name,'/')[5] as varchar)  feed, '".$proc_date_day."' date, count(*) cdr_count, count(distinct file_name) file_count, date_format(current_timestamp, '%Y%m%d %H:%i:%s') timestamp, tbl_dt from flare_8.sdp_dmp_ma where tbl_dt=".$proc_date." group by tbl_dt,  '".$proc_date_day."', split(file_name,'/')[5];";


$sql_stmt2 = "insert into nigeria.tmp_sdp_cdr_trend2 
select  cast(split(file_name,'/')[5] as varchar)  feed, '".$proc_date_day."' date, count(*) cdr_count, count(distinct file_name) file_count, date_format(current_timestamp, '%Y%m%d %H:%i:%s') timestamp, tbl_dt from flare_8.sdp_dmp_da where tbl_dt=".$proc_date." group by tbl_dt,  '".$proc_date_day."', split(file_name,'/')[5];";

$sql_stmt3 = "insert into nigeria.tmp_sdp_cdr_trend3 
select  cast(split(file_name,'/')[5] as varchar)  feed, '".$proc_date_day."' date, count(*) cdr_count, count(distinct file_name) file_count, date_format(current_timestamp, '%Y%m%d %H:%i:%s') timestamp, tbl_dt from flare_8.cs5_sdp_acc_adj_ma where tbl_dt=".$proc_date." group by tbl_dt,  '".$proc_date_day."', split(file_name,'/')[5];";

$sql_stmt4 = "insert into nigeria.tmp_sdp_cdr_trend4
select  cast(split(file_name,'/')[5] as varchar)  feed, '".$proc_date_day."' date, count(*) cdr_count, count(distinct file_name) file_count, date_format(current_timestamp, '%Y%m%d %H:%i:%s') timestamp, tbl_dt from flare_8.cs5_sdp_acc_adj_da where tbl_dt=".$proc_date." group by tbl_dt,  '".$proc_date_day."', split(file_name,'/')[5];";

$sql_stmt5 = "insert into nigeria.tmp_sdp_cdr_trend5
select  cast(split(file_name,'/')[5] as varchar)  feed, '".$proc_date_day."' date, count(*) cdr_count, count(distinct file_name) file_count, date_format(current_timestamp, '%Y%m%d %H:%i:%s') timestamp, tbl_dt from flare_8.cs5_sdp_acc_adj_ac where tbl_dt=".$proc_date." group by tbl_dt,  '".$proc_date_day."', split(file_name,'/')[5];";

## Insert records into temp table and confirm that tables is sucessfully updated
$test = "Yes";

while( $test eq "Yes")
{
      ##print "\nExecuting SQL Statement\n$sql_stmt\n";
      $cmd =`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt1"`;
			`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt2"`;
			`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt3"`;
			`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt4"`;
			`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt5"`;
			
		print "\nTemp Insert completed for ".$proc_date."\n";

	  $check_sql="select count(1) cnt from  nigeria.tmp_sdp_cdr_trend1";
	  
      $rec_cnt = `/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$check_sql"`;	
      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);
 
      if ($rec_cnt != 0 )
      {
		print "\n Temp Count is greater than 0 for ".$proc_date."\n";
        $test = "No";
      }
      else
      {
		print "\nTemp Count equals 0 for ".$proc_date." Trying again\n";
        print "\nSleeping for 90 Secs\n";
        sleep 90;
      }
}





## COLLATION OF RESULTS
## Insert records into Main table and confirm that tables is sucessfully updated
system('hdfs dfs -rm hdfs://ngdaas/user/hive/nigeria/sdp_cdr_trend/tbl_dt='.$proc_date.'/*');
##$rms= $rms.$proc_date.'/*';

system('hdfs dfs -mkdir hdfs://ngdaas/user/hive/nigeria/sdp_cdr_trend/tbl_dt='.$proc_date);
##msck repair table nigeria.aod_summary
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');
system('hive -e "msck repair table nigeria.sdp_cdr_trend"');

$sql_stmt = "insert into nigeria.sdp_cdr_trend
	  select * from nigeria.tmp_msc_ccn_trend1 
	  union all select * from nigeria.tmp_sdp_cdr_trend1 
	  union all select * from nigeria.tmp_sdp_cdr_trend2
	  union all select * from nigeria.tmp_sdp_cdr_trend3
	  union all select * from nigeria.tmp_sdp_cdr_trend4
	  union all select * from nigeria.tmp_sdp_cdr_trend5
	  ";

$test = "Yes";

while( $test eq "Yes")

{

      ##print "\nExecuting SQL Statement\n$sql_stmt\n";

      $cmd =`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt"`;

		print "\nOriginal insert for ".$proc_date." Completed \n";

      $check_sql = "select count(1) cnt from nigeria.msc_ccn_trend  where tbl_dt=".$proc_date.";";

      $rec_cnt = `/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$check_sql"`;

      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);

 

      if ($rec_cnt != 0 )

      {
		print "\nTrend analysis successfully completed for ".$proc_date." \n";
        $test = "No";

      }

      else

      {
		print "\nOriginal Count equals 0 for ".$proc_date." Trying again\n";
        print "\nSleeping for 90 Secs\n";

        sleep 90;

      }

}


}
}