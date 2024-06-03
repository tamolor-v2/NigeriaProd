
DATE=$(date -d '-2 day' '+%Y%m%d')
mkdir /nas/share05/tools/Engine_Room/KYC_Marketing/logs/$(date '+%Y%m%d')
bash /nas/share05/tools/Engine_Room/KYC_Marketing/Insert_KYC_Marketing_yest.sh | tee /nas/share05/tools/Engine_Room/KYC_Marketing/logs/$(date '+%Y%m%d')/yest_Insert_${DATE}.log
