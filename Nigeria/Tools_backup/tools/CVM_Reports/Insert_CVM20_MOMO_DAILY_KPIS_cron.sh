
RunDate=$(date -d '-1 day' '+%Y%m%d')
DATE=$(date '+%Y%m%d')
DDATE=$(date '+%Y%m%d%H%m%S')
mkdir /nas/share05/tools/CVM_Reports/logs/$DATE
bash /nas/share05/tools/CVM_Reports/Insert_CVM20_MOMO_DAILY_KPIS_base.sh $RunDate | tee /nas/share05/tools/CVM_Reports/logs/$DATE/Insert_CVM20_MOMO_DAILY_KPIS_${DDATE}_${RunDate}_cron.log
