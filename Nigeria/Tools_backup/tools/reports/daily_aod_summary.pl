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
my $sql_stmt;
my $filename;
my $cmd;
my $dir  = "/home/wrasheed/scripts";
my $test;
my $check_sql;
my $rec_cnt;
my $i ;
my $proc_date;
my $rms;

## Procedure to generate daily AOD Daily summary

{

system('clear');


$cmd =  `cd $dir`;
my $pc = `ps -ef |grep daily_aod_summary.pl |wc -l`;
$pc =~ s/^\s+//;  ## strip leading and trailing spaces
chomp($pc);      

if ($pc gt 3 ) { print "\nDaily AOD job is currently running\n"; exit; }  ## exit if sample job is already running


print "\nSummarising AOD\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');
print "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";

## Drop table in hive
##system('hive -e "drop table nigeria.tmp_aod_summary"');

for ($i=1; $i <=7; $i++) 
{

$check_sql   = "select CAST(date_format(date_add('day', - $i , current_date ),'%Y%m%d') AS INT) date;";
$proc_date   = `/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$check_sql"`;
##$proc_date = s/\"//g ; 
##$proc_date = s/date//g ; 
$proc_date =~ s/[\r\n\r]+//g; 
chomp($proc_date);

$sql_stmt ="delete from nigeria.tmp_aod_summary";
print "Temp table successfully deleted for ".$proc_date."\n";

$cmd =`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags wrasheed --execute "$sql_stmt"`;

$sql_stmt = "insert into nigeria.tmp_aod_summary 
SELECT 
'daily' AGGR,
cast(date_format(current_date,'%Y%m') as int) MONTH,
cast(SUBSTR(ORIGINTRANSACTIONID,1,4) as int) BANK_CODE,
B.NAME BANK_DESCRIPTION,
cast(MAX(C.RATE) as decimal) BANK_RATE,
cast(SUM(cast(TRANSACTIONAMOUNT as decimal)) as decimal) TOTAL_LOAD_AMOUNT ,
COUNT(*) RECORD_COUNT,
SUM(cast(TRANSACTIONAMOUNT as decimal)*cast(C.RATE as bigint)/100) DISCOUNT_BANK ,
SUM(cast(TRANSACTIONAMOUNT as decimal)*0.2/100) QRIOS_DISCOUNT ,
SUM(cast(TRANSACTIONAMOUNT as decimal)*(cast(C.RATE as bigint)+.2)/100) TOTAL_DISCOUNT ,
cast(SUM(cast(TRANSACTIONAMOUNT as decimal))/1.05*.05 as decimal) VAT ,
cast (SUM(cast(TRANSACTIONAMOUNT as decimal)) /1.05 as decimal) UNEARNED_REVENUE ,
SUM(cast(TRANSACTIONAMOUNT as decimal)*(1-(cast(C.RATE as bigint)+.2)/100)) AMOUNT_RECEIVABLE_FROM_BANK,
cast(".$proc_date." as int) tbl_dt
FROM flare_8.CS5_AIR_REFILL_MA A,
nigeria.bankcode_lookup B ,
nigeria.bankrate_lookup C
WHERE ORIGINNODETYPE = 'HWSDP' AND ORIGINHOSTNAME = 'SMS'
AND cast(SUBSTR(ORIGINTRANSACTIONID,1,4) as int) = B.CD
AND cast(B.CD as int) = cast(C.CD as int)
AND tbl_dt = cast(date_format(date_add('day', -1, current_date ),'%Y%m%d') as int)
GROUP BY 
B.NAME,
cast(SUBSTR(ORIGINTRANSACTIONID,1,4) as int),
date_format(current_date,'%Y%m'),
cast(date_format(date_add('day', -1, current_date ),'%Y%m%d') as int);";

 

 

## Insert records into temp table and confirm that tables is sucessfully updated

$test = "Yes";

while( $test eq "Yes")

{

      ##print "\nExecuting SQL Statement\n$sql_stmt\n";

      $cmd =`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt"`;
		
		print "\nTemp Insert completed for ".$proc_date."\n";
 

      $check_sql = "select count(1) cnt from nigeria.tmp_aod_summary";
		
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



## Insert records into Main table and confirm that tables is sucessfully updated
system('hdfs dfs -rm hdfs://ngdaas/apps/hive/warehouse/nigeria.db/aod_summary/tbl_dt='.$proc_date.'/*');
##$rms= $rms.$proc_date.'/*';

system('hdfs dfs -mkdir hdfs://ngdaas/apps/hive/warehouse/nigeria.db/aod_summary/tbl_dt='.$proc_date);
##msck repair table nigeria.aod_summary
system('hive -e "msck repair table nigeria.aod_summary"');

##system ($rms);
##system('hdfs dfs -rm hdfs://ngdaas/apps/hive/warehouse/nigeria.db/aod_summary/tbl_dt=`$proc_date`/');
$sql_stmt = "insert into nigeria.aod_summary select * from nigeria.tmp_aod_summary;";

$test = "Yes";

while( $test eq "Yes")

{

      ##print "\nExecuting SQL Statement\n$sql_stmt\n";

      $cmd =`/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$sql_stmt"`;

		print "\nOriginal insert for ".$proc_date." Completed \n";

      $check_sql = "select count(1) cnt from nigeria.aod_summary  where tbl_dt=".$proc_date.";";

      $rec_cnt = `/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER --client-tags wrasheed --execute "$check_sql"`;

      $rec_cnt =~ s/\"//g ; $rec_cnt =~ s/cnt//g ; $rec_cnt =~ s/[\r\n\r]+//g; chomp($rec_cnt);

 

      if ($rec_cnt != 0 )

      {
		print "\nOriginal Count is greater than 0 for ".$proc_date." \n";
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