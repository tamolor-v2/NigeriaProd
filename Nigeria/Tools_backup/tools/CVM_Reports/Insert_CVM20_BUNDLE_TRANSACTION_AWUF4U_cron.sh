
RunDate=$(date -d '-1 day' '+%Y%m%d')
DATE=$(date '+%Y%m%d%H%m%S')
mkdir /nas/share05/tools/CVM_Reports/logs/$RunDate
bash /nas/share05/tools/CVM_Reports/Insert_CVM20_BUNDLE_TRANSACTION_AWUF4U_base.sh $RunDate | tee /nas/share05/tools/CVM_Reports/logs/$RunDate/Insert_CVM20_BUNDLE_TRANSACTION_AWUF4U_${DATE}_${RunDate}_cron.log
