mytime=$(date +"%Y-%m-%d_%H-%M-%S")
yest=$(date -d '-1 day' '+%Y%m%d') 
removeDir=$(date -d '-60 day' '+%Y%m%d')

mkdir -p /nas/share05/tools/Crontab/logs/validationCheck/${yest}
rm -r /nas/share05/tools/Crontab/logs/validationCheck/${removeDir}
time spark-submit --verbose --num-executors 10 --executor-cores 1 --executor-memory 4g --class com.ligadata.main.NumberChecker --master local[*] --conf "spark.ui.enabled=false" /nas/share05/tools/validationCheck/jar_test/NumberChecker_2.10-1.0.jar -v -c /nas/share05/tools/validationCheck/config/validationCheck_BSL_new_test.json 2>&1 | tee "/nas/share05/tools/Crontab/logs/validationCheck/${yest}/validation_check_${mytime}.log"
