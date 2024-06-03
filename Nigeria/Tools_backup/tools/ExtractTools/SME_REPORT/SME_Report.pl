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

##-----set variable values here 
## $sql_main - the sql statement to be executed should be assigned to this later in the code below the declaration section
## $hive_root  - hive root is set here for usage, there may be no need to change this unless you have to 
## $dir  - this is set to normal/planned ops directory for the report execution, it will be set but can be changed depending
## $schema - schema name, preset to nigeria, the planned schema for operational opco reporting


## These (below) definitely must be changed
my $proc_name = "Prepaid_Activation.pl" ; #add script name here, change to appropriate name
my $proc_short_desc = "Prepaid_Activation" ; #Short Script Description here pls


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
print "\nPROCESSING FOR $proc_date \n";

$sql_stmt = "
delete from nigeria.SME_REPORTS where tbl_dt = $proc_date
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.SME_REPORTS
SELECT
  msisdn
, businesstype
, numberofemployees
, branch
, lastactiondate
, tbl_dt
FROM
flare_8.sme_reports
where tbl_dt = $proc_date
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;


print "\nEND OF PROCESSING\n"; 

}
}
