##!usr/bin/env

# Report Description 
# Template to be used to execute report to be stored in a partition table
# The table must created first separate statement on presto or hive, preferrably presto
# Replace the first two lines with description of the report/module
 
# Developer Names:
# Support Personnel Name
# Date Last modified
# Change History with Dates

#Parameters: Start Date and End Date in that order

#use strict;
use FileHandle;
use Getopt::Long;
use IO::Handle;
use IO::Socket;

# Advance variables start
# all variables in this enclosure are for advance 
my @months      = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my @months      = qw(01 02 03 04 05 06 07 08 09 10 11 12);
my @weekDays    = qw(Sun Mon Tue Wed Thu Fri Sat Sun);
my @dates;

my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
my $year = 1900 + $yearOffset;
my $theTime = "$hour:$minute:$second, $weekDays[$dayOfWeek] $months[$month] $dayOfMonth, $year";
my $epoc = time();
my $check_sql;
# Advance variables end

my $rec_cnt;

#parameter from command lines.
my $values = @ARGV;
my $value_cnt=scalar $values;
my $start_date   = $ARGV[0];
my $end_date   = $ARGV[0];

#-----set variable values here 
# $sql_main - the sql statement to be executed should be assigned to this later in the code below the declaration section
# $hive_root  - hive root is set here for usage, there may be no need to change this unless you have to 
# $dir  - this is set to normal/planned ops directory for the report execution, it will be set but can be changed depending
# $schema - schema name, preset to nigeria, the planned schema for operational opco reporting


# These (below) definitely must be changed
my $proc_name = "Bulk_USSD.pl" ; #add script name here, change to appropriate name
my $proc_short_desc = " Bulk_USSD" ; #Short Script Description here pls


{
system('clear');

#check if this module/report is already running and stop the current execution if yes
my $pc = `ps -ef |$proc_name|wc -l`;
$pc =~ s/^\s+//;   #strip leading and trailing spaces
chomp($pc);      
 
if ($pc gt 3 ) { print "\n$proc_short_desc job is currently running\n"; exit; }   #exit if sample job is already running

if ($value_cnt gt 1 ) {
    $end_date = $ARGV[1];
 }

print "\nStarting Trend\n";
print "Running: kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n";
system('kinit -kt /etc/security/keytabs/daasuser.keytab daasuser\@MTN.COM\n');

#the loop to execute a date at a time 
#note that start and end date must be within a month, the process cannot handle dates across months

for ($proc_date=$start_date; $proc_date <=$end_date; $proc_date++) 
{
print "\nPROCESSING FOR $proc_date \n";


$sql_stmt = "
delete from nigeria.bulk_ussd_tmp
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.bulk_ussd_tmp 
select tbl_dt date_of_transactions,chrononumber,mobilenumber,servedaccount,procedures partner_name,ussd_code,vascode,dedicatedaccountid,
try_cast(dedicatedaccountvaluebeforecall as double) opening_balance,try_cast(dedicatedamountused as double) used_vlume,
try_cast(dedicatedaccountvalueaftercall as double) closed_balance,originatingservices
from flare_8.cs5_ccn_gprs_ma x
left join nigeria.ussd_msisdn_provider y
on (substr(trim(mobilenumber),-10) = trim(provider_msisdn))
where tbl_dt = $proc_date
and  (upper(chargingcontextid) LIKE '%SCAP%')
and  originatingservices = 'USSDGW'
and serviceclassid in ('228','229')
and try_cast(costofsession as double) > 0
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;



$sql_stmt = "
delete from nigeria.bulk_ussd_tmp2
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
insert into nigeria.bulk_ussd_tmp2 
select date_of_transactions,mobilenumber,partner_name,min(chrononumber) min_chrononumber,dedicatedaccountid,max(chrononumber) max_chrononumber
from nigeria.bulk_ussd_tmp
group by date_of_transactions,mobilenumber,partner_name,dedicatedaccountid
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
delete from nigeria.bulk_ussd_tmp3
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;



$sql_stmt = "
insert into nigeria.bulk_ussd_tmp3
select date_of_transactions,mobilenumber,partner_name,ussd_code,originatingservices,dedicatedaccountid,sum(used_vlume) used_vlume
from nigeria.bulk_ussd_tmp
group by date_of_transactions,mobilenumber,partner_name,ussd_code,originatingservices,dedicatedaccountid
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;


$sql_stmt = "
delete from nigeria.bulk_ussd_report where tbl_dt = $proc_date
;";

$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;

$sql_stmt = "
insert into nigeria.bulk_ussd_report
select x.date_of_transactions,x.mobilenumber,x.ussd_code,x.partner_name,adjustmentamount,
opening_balance,used_vlume,closed_balance,expirydate,x.date_of_transactions tbl_dt
from nigeria.bulk_ussd_tmp3 x
join 
(
select a.date_of_transactions,a.mobilenumber,a.partner_name,a.dedicatedaccountid,opening_balance
from nigeria.bulk_ussd_tmp2 a,nigeria.bulk_ussd_tmp b 
where a.mobilenumber = b.mobilenumber 
and   a.min_chrononumber = chrononumber
and   a.partner_name = b.partner_name 
and   a.dedicatedaccountid = b.dedicatedaccountid
) y on (x.mobilenumber = y.mobilenumber and x.partner_name = y.partner_name and x.dedicatedaccountid = y.dedicatedaccountid)
join
(
select a.date_of_transactions,a.mobilenumber,a.partner_name,a.dedicatedaccountid,closed_balance
from nigeria.bulk_ussd_tmp2 a,nigeria.bulk_ussd_tmp b 
where a.mobilenumber = b.mobilenumber 
and   a.max_chrononumber = chrononumber
and   a.partner_name = b.partner_name 
and   a.dedicatedaccountid = b.dedicatedaccountid
) z on (x.mobilenumber = z.mobilenumber and x.partner_name = z.partner_name and x.dedicatedaccountid = z.dedicatedaccountid)
left join
(
select tbl_dt,accountid,dedicatedaccountid,expirydate
from flare_8.sdp_dmp_da
where tbl_dt = $proc_date
) m
on (trim(m.accountid) = substr(trim(x.mobilenumber),-10) and m.dedicatedaccountid = x.dedicatedaccountid)
left join
(
select tbl_dt,accountnumber,dedicatedaccountid,try_cast(adjustmentamount as double)  adjustmentamount
from flare_8.cs5_sdp_acc_adj_da x
where tbl_dt = $proc_date
) n
on (trim(n.accountnumber) = substr(trim(x.mobilenumber),-10) and n.dedicatedaccountid = x.dedicatedaccountid)
;";


$cmd =`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --output-format CSV --client-tags isarungi --execute "Start transaction; $sql_stmt;commit;"`;

print "\nEND OF PROCESSING\n"; 

}
}
