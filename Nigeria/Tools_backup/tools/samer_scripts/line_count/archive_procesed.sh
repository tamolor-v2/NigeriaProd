#CS5_AIR_ADJ_DA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/AIR_ADJ_DA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/AIR_ADJ_DA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_AIR_ADJ_DA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_AIR_ADJ_DA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SDP_AIR_ADJ_DA_$mytime.log"
---------------
#CS5_AIR_ADJ_MA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/AIR_ADJ_MA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/AIR_ADJ_MA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_AIR_ADJ_MA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_AIR_ADJ_MA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SDP_AIR_ADJ_MA_$mytime.log"
---------------

#GGSN_CDR
#=============
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/GGSN/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/GGSN/processed -rg "^([0-9]{8})_([0-9]{2}).*_(GGSN_CDR)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_GGSN_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_GGSN_$mytime.log"
-----------------
#MSC_CDR
#=============

mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/MSC/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/MSC/processed -rg "^([0-9]{8})_([0-9]{2}).*_(MSC_CDR)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_MSC_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_MSC_$mytime.log"
-----------------
#CS5_CCN_GPRS_MA
#=============

mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_GPRS_MA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_GPRS_MA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_GPRS_DA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_GPRS_MA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_GPRS_MA_$mytime.log"
--------------
#CS5_CCN_GPRS_DA
#============

mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_GPRS_DA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_GPRS_DA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_GPRS_DA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_GPRS_DA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_GPRS_DA_$mytime.log"
------------------
#CS5_CCN_GPRS_AC
#===============

mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_GPRS_AC/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_GPRS_AC/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_GPRS_AC)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_GPRS_AC_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_GPRS_AC_$mytime.log"
------------------
#CS5_CCN_SMS_AC
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_SMS_AC/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_SMS_AC/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_SMS_AC)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_SMS_AC_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SMS_AC_$mytime.log"
----------------
#CS5_CCN_SMS_DA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_SMS_DA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_SMS_DA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_SMS_DA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_SMS_DA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SMS_DA_$mytime.log"
---------------
#CS5_CCN_SMS_MA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_SMS_MA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_SMS_MA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_SMS_MA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_SMS_MA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SMS_MA_$mytime.log"
---------------
#CS5_CCN_VOICE_DA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_VOICE_DA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_VOICE_DA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_VOICE_DA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_VOICE_DA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_VOICE_DA_$mytime.log"
---------------
#CS5_CCN_VOICE_MA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/CCN_VOICE_MA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/CCN_VOICE_MA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_CCN_VOICE_MA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_VOICE_MA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_VOICE_MA_$mytime.log"
---------------
#CS5_SDP_ACC_ADJ_AC
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/SDP_ADJ_AC/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/SDP_ACC_ADJ_AC/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_SDP_ACC_ADJ_AC)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_SDP_ACC_ADJ_AC_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SDP_ACC_ADJ_AC_$mytime.log"
---------------
#CS5_SDP_ACC_ADJ_DA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/SDP_ADJ_DA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/SDP_ACC_ADJ_DA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_SDP_ACC_ADJ_DA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_SDP_ACC_ADJ_DA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SDP_ACC_ADJ_DA_$mytime.log"
---------------
#CS5_SDP_ACC_ADJ_MA
#===========
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/FlareData/CDR/SDP_ACC_ADJ_MA/processed  -out /mnt/beegfs/tmp/waddah/tmp -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/FlareData/CDR/SDP_ACC_ADJ_MA/processed -rg "^([0-9]{8})_([0-9]{2}).*_(#CS5_SDP_ACC_ADJ_MA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_BIB_files_SDP_ACC_ADJ_MA_ 2>&1 | tee "/mnt/beegfs/share/tmp/waddah/tmp/move_CDR_processed_arch_SDP_ACC_ADJ_MA_$mytime.log"
---------------

