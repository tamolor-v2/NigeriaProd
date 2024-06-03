mytime=$(date +"%Y-%m-%d_%H-%M-%S")
  
time spark-submit --verbose --num-executors 10 --executor-cores 1 --executor-memory 40g --conf spark.yarn.executor.memoryOverhead=15096 --driver-memory 25g  --class com.ligadata.main.NumberChecker --conf spark.yarn.driver.memoryOverhead=15048 /mnt/beegfs/tools/trendingFeeds/jar/NumberChecker_2.10-1.0.jar -t -c /mnt/beegfs/tools/trendingFeeds/config/validationCheck.json 2>&1 | tee "/mnt/beegfs/tools/trendingFeeds/log/validation_check_${mytime}.log"
