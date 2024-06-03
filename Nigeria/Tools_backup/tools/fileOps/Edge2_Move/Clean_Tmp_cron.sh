from=$(date -d '-1000 day' '+%Y/%m/%d-00:00:00')
#to=$(date -d '-1 day' '+%Y/%m/%d-23:59:59')
to=$(date --date '-6 hours' +"%Y/%m/%d-%H:%M:%S")
pid='Backup_LZ_files'
today=$(%Y%m%d)
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
log_path=/mnt/beegfs_bsl/Backup_LZ_files/logs
report_path=/mnt/beegfs_bsl/Backup_LZ_files/report

mkdir -p  /mnt/beegfs_bsl/Backup_LZ_files/logs/${today}


time java -Xmx8g -Xms8g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT.jar -in /data/data_lz/beegfs/live -nt 32 -out $report_path/${today} -dp 15 -tf 1 -op delete -nosim -iffl "^.*\.(_COPYING_|tmp|TMP)$" -iffl "^\..*" -lbl delete  -fbd $from $to -of -od 2>&1 | tee "$log_path/${today}/delete_tmp_lz_$mytime.log"

