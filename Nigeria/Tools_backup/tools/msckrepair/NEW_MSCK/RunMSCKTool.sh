DAY=$(date -d "$date -1  day" +%Y%m%d)
cd /nas/share05/tools/msckrepair/NEW_MSCK
 mkdir -p /nas/share05/tools/msckrepair/NEW_MSCK/logs/${DAY}
java -cp MsckRepair-assembly-0.1.jar com.ligadata.utilities.msck.DependencyValidator  --configFile /nas/share05/tools/msckrepair/NEW_MSCK/msck_config.conf --dates ${DAY} --totalThreads "5"  --msckFile "" --depValidate --msckRepair  2>&1 | tee  /nas/share05/tools/msckrepair/NEW_MSCK/logs/${DAY}/msck_repair_$(date +%Y%m%d_%H%M%S).log
