mytime=$(date +"%Y-%m-%d_%H-%M-%S")
  
time spark-submit --verbose --num-executors 10 --executor-cores 1 --executor-memory 4g --class com.ligadata.main.NumberChecker --master local[*] --conf "spark.ui.enabled=false" /mnt/beegfs/tools/validationCheck/jar/NumberChecker_2.10-1.0.jar -v -c /mnt/beegfs/tools/validationCheck/config/validationCheck_rerun.json 2>&1 | tee "/mnt/beegfs/yousef/validationCheck/report/validation_check_${mytime}.log"
