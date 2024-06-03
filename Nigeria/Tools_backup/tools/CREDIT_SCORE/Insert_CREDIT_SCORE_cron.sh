
RunDate=$(date -d '-1 day' '+%Y%m%d')
DATE=$(date '+%Y%m%d')
DDATE=$(date '+%Y%m%d%H%m%S')
mkdir /nas/share05/tools/CREDIT_SCORE/logs/$DATE
bash /nas/share05/tools/CREDIT_SCORE/Insert_CREDIT_SCORE_base.sh $RunDate | tee /nas/share05/tools/CREDIT_SCORE/logs/$DATE/Insert_CREDIT_SCORE_${DDATE}_${RunDate}_cron.log
