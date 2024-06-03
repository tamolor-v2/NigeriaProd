
dt=`date '+%Y%m%d%H%M%S'`
###spark-submit --num-executors 10 --executor-cores 20 --executor-memory 40g --class com.ligadata.main.ColumnsCountSummary --master yarn ../jar/NumberChecker_2.10-1.0.jar -c ../config/configFile.json  2>&1 | tee ../logs/$dt.log

spark-submit  --num-executors 22 --executor-cores 3 --executor-memory 40g --conf spark.yarn.executor.memoryOverhead=25096 --driver-memory 25g --conf spark.yarn.driver.memoryOverhead=25048 --class com.ligadata.main.ColumnsCountSummary --master yarn ../jar/NumberChecker_2.10-1.0.jar  -c ../config/configFile.json  2>&1 | tee ../logs/$dt.log
