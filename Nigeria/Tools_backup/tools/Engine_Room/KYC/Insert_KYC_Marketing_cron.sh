
DATE=$(date -d '-1 day' '+%Y%m%d')
mkdir /nas/share05/tools/Engine_Room/KYC_Marketing/logs/$(date '+%Y%m%d')
bash /nas/share05/tools/Engine_Room/KYC_Marketing/Insert_KYC_Marketing.sh | tee /nas/share05/tools/Engine_Room/KYC_Marketing/logs/$(date '+%Y%m%d')/Insert_${DATE}.log
