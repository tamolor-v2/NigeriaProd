
RunDate=$(date -d '-7 day' '+%Y%m%d')
DATE=$(date '+%Y%m%d')
DDATE=$(date '+%Y%m%d%H%m%S')
mkdir /nas/share05/tools/CVM_Reports/logs/$DATE
bash /nas/share05/tools/CVM_Reports/Insert_CVM20_DEVICEINFO_base.sh $RunDate | tee /nas/share05/tools/CVM_Reports/logs/$DATE/Insert_CVM20_DEVICEINFO_${DDATE}_${RunDate}_cron.log
