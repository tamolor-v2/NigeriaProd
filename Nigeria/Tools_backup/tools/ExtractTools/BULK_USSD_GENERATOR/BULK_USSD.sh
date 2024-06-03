#!bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')
cd /nas/share05/tools/ExtractTools/BULK_USSD_GENERATOR/
perl /nas/share05/tools/ExtractTools/BULK_USSD_GENERATOR/BULK_USSD.pl $yest
