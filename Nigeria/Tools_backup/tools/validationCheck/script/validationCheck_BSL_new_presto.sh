mytime=$(date +"%Y-%m-%d_%H-%M-%S")
yest=$(date -d '-1 day' '+%Y%m%d') 
removeDir=$(date -d '-60 day' '+%Y%m%d')

mkdir -p /nas/share05/tools/Crontab/logs/validationCheck/${yest}
rm -r /nas/share05/tools/Crontab/logs/validationCheck/${removeDir}
currentDir=/nas/share05/tools/validationCheck/script
time java -Dlog4j.configurationFile=${currentDir}/../config/log4j2.xml -cp "/mnt/beegfs_bsl/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar:/nas/share05/tools/validationCheck/jar/NumberChecker_2.10-1.0_presto.jar" com.ligadata.main.NumberChecker -v -c /nas/share05/tools/validationCheck/config/validationCheck_BSL_new_presto.json 2>&1 | tee "/nas/share05/tools/Crontab/logs/validationCheck/${yest}/validation_check_${mytime}.log"
