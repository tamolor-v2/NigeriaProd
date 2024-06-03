
DATE=$1
mkdir /nas/share05/tools/Engine_Room/KYC_Marketing/logs/$(date '+%Y%m%d')
bash /nas/share05/tools/Engine_Room/KYC_Marketing/Insert_KYC_Marketing_cron_OneDate.sh $DATE | tee /nas/share05/tools/Engine_Room/KYC_Marketing/logs/$(date '+%Y%m%d')/Insert_Rerun_${DATE}.log
