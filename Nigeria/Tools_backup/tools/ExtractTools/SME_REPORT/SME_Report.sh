#!/bin/bash

date=$(date +"%Y%m%d")
yest=$(date -d '-1 day' '+%Y%m%d')
cd /nas/share05/tools/ExtractTools/SME_REPORT/
perl /nas/share05/tools/ExtractTools/SME_REPORT/SME_Report.pl $yest | tee /nas/share05/tools/ExtractTools/SME_REPORT/logs/SME_Report_${date}.log
