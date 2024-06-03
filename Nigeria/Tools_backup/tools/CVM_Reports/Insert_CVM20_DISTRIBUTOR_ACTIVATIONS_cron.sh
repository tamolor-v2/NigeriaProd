
RunDate=$(date -d '-1 day' '+%Y%m%d') #$(date '+%Y%m%d')
DATE=$(date '+%Y%m%d%H%m%S')
mkdir /nas/share05/tools/CVM_Reports/logs/$RunDate
bash /nas/share05/tools/CVM_Reports/Insert_CVM20_DISTRIBUTOR_ACTIVATIONS_base.sh 20110130 | tee /nas/share05/tools/CVM_Reports/logs/$RunDate/Insert_CVM20_DISTRIBUTOR_ACTIVATIONS_${DATE}_${RunDate}_cron.log
