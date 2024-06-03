mytime=$(date +"%Y-%m-%d_%H-%M-%S")
yest=$(date -d '-1 day' '+%Y%m%d') 
removeDir=$(date -d '-60 day' '+%Y%m%d')

mkdir -p /mnt/beegfs/tools/Crontab/logs/validationCheck/${yest}
rm -r /mnt/beegfs/tools/Crontab/logs/validationCheck/${removeDir}
time spark-submit --verbose --num-executors 10 --executor-cores 1 --executor-memory 4g --class com.ligadata.main.NumberChecker --master local[*] --conf "spark.ui.enabled=false" /mnt/beegfs/tools/validationCheck/jar/NumberChecker_2.10-1.0.jar -v -c /mnt/beegfs/tools/validationCheck/config/validationCheck_BSL.json 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/validationCheck/${yest}/validation_check_${mytime}.log"
