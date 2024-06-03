
RunDate=$1
DATE=$(date '+%Y%m%d')
DDATE=$(date '+%Y%m%d%H%m%S')
mkdir /nas/share05/tools/CVM_Reports/logs/$DATE
bash /nas/share05/tools/CVM_Reports/Insert_CVM20_CAMPAIGN_RELATED_ATTRIBUTES_OUTBOUND_base.sh $RunDate | tee /nas/share05/tools/CVM_Reports/logs/$DATE/Insert_CVM20_CAMPAIGN_RELATED_ATTRIBUTES_OUTBOUND_${DDATE}_${RunDate}_rerun.log
