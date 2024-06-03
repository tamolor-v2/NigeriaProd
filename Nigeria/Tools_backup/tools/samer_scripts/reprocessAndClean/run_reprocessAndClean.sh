 #!/bin/bash
#FEED=$2
#CONF=$1
#DATE=$3
#main
#echo "$CONF" "$FEED" "$DATE"
#main_func "$CONF" "$FEED" "$DATE"
#main_func(){
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] [--hdfs]";}
remove_duplicates(){
		FEED=$1
		Schema=$2
		cols=$(hive -e  "SHOW COLUMNS IN ${Schema}.${FEED};"| awk -F" " '{print $1","}'| sed '/tbl_dt,/d')
                hiveMem="set tez.am.resource.memory.mb = 3048;"
                container="set hive.tez.container.size=40096;"
                partition="set hive.exec.dynamic.partition.mode=nonstrict;"
                query="INSERT OVERWRITE TABLE ${Schema}.${FEED} partition (tbl_dt)"
                query="${query} select ${cols} tbl_dt from"
                query="${query} (select *,row_number() over( partition by file_name,file_offset) seq from ${Schema}.${FEED} where tbl_dt =${DATE}) c where c.seq=1;"
                #echo "$query"
                echo -e "${hiveMem}\n${partition}\n${container}\n${query}">./${FEED}.hql
}
save_prevPartitions(){
DATE=$(date -d "$1" +"%Y%m%d")
prev=$(date -d "$DATE 1 day ago" +%Y%m%d)
FEED=$2
currentDate=$3
echo "$DATE ----> $FEED"
echo "$prev"
file_lst=`hadoop fs -ls $commonHdfsPath/$FEED/tbl_dt=$prev | awk '{print $NF}' | grep .dat$ | tr '\n' ' '`
#file_lst=($(hadoop fs -ls -R ${commonHdfsPath}/$FEED/tbl_dt=${prev}))
echo $file_lst | tr " " "\n" >"./${FEED}_${prev}_${currentDate}.txt"
#for file_name in $file_lst 
#do 
#echo $file_name
#done
#declare -p $file_lst
#echo ${file_lst[@]}
}
currentDate=`date +%Y%m%d%H%M%S`
#echo "$currentDate"
#FEED=
#CONF=
#DATE=
Schema=
HDFS_BACK=
commonHdfsPath=/FlareData/DEV
commonArchivedPath=/mnt/beegfs/production/archived
FromLocation=
HDFSBackupPath=/FlareData/ReprocessBackup
if [[ $# -eq 0 ]]; then
Usage;exit -1
fi
while [[ $# -gt 0 ]]
        do
        case $1 in
        --config)
        CONF=$2
        shift
        ;;
        --feed)
        FEED=$2
        shift
        ;;
        --date)
        DATE=(${2/,/ })
        shift
        ;;
        --schema)
        Schema=$2
        shift
        ;;

        *)
        Usage;exit -1
        shift
        ;;
        esac
        shift
done
index=0
#file_lst=($(hadoop fs -ls -R ${commonHdfsPath}/$FEED/tbl_dt=${DATE}))
reprocessingDate=$(date -d "$DATE" +"%Y%m%d")

index=0
while read line ; do
        MYARRAY[$index]="$line"
        index=$(($index+1))
done < $CONF
declare -A matrix
#IFS=','
for ((i=1;i<index;i++)) do
        line=${MYARRAY[$i]}
#read -ra feed_details <<< "$line"
IFS=', ' read -r -a feed_details <<< "$line"
#readarray -td ',' feed_details <<<"$line"; declare -p a;
#feed_details=($(echo "$line" | tr ',' '\n'))
        feed_name=${feed_details[0]}
        daas_processing_path=${feed_details[1]}
        bib_path=${feed_details[2]}
        hdfs_path=${feed_details[3]}
        daas_path=${feed_details[4]}
        flare_cluster_path=${feed_details[5]}
        daas_processed_path=${feed_details[6]}
        matrix[$i,0]=$feed_name
        matrix[$i,1]=$daas_processing_path
        matrix[$i,2]=$bib_path
        matrix[$i,3]=$hdfs_path
        matrix[$i,4]=$daas_path
        matrix[$i,5]=$flare_cluster_path
        matrix[$i,6]=$daas_processed_path
done
feed_loc="99999"
for ((i=1;i<index;i++)) do
if [[ ${matrix[$i,0]} == $FEED ]]
then
feed_loc=$i
fi
done
if [ ${feed_loc} != "99999" ]
then
        if [ ${DATE[0]} != "all" ]
        then
        #echo "hadoop fs -mkdir -p /FlareData/backup_before_bib/$FEED"
        #hadoop fs -mkdir -p /FlareData/backup_before_bib/$FEED
        #echo "mkdir -p /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}"
        #mkdir -p /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}
	save_prevPartitions "$DATE" "$FEED" "$currentDate"
	exit
		echo "#$query"
		FromLocation=""
		archived_file_count=($(find  ${matrix[$feed_loc,2]}/${DATE} -type f -name "*${DATE}*" |wc -l))
                processed_file_count=($(find  ${matrix[$feed_loc,6]}${DATE} -type f -name "*${DATE}*" |wc -l))
                        if [[ $archived_file_count >0 && $processed_file_count>0 ]]
                        then
                                echo "archived_file_count: $spool_file_count, processed_file_count: $processed_file_count"
                                echo "Files exist n iarchived and processed, Exitting ......"
                                exit -1
                        fi
                if [  -d "${matrix[$feed_loc,6]}${DATE}" ]
                then
                	FromLocation=${matrix[$feed_loc,6]}${DATE}
                        echo "$FromLocation exists. taking it"
                elif [  -d "{matrix[$feed_loc,2]}/${DATE}" ]
                then
                 	FromLocation={matrix[$feed_loc,2]}/${DATE}
                 	echo "$FromLocation exists. taking it"
                else
			echo "Source doesn't exist, skipping reprocess"
			exit -1
		fi
                if [  -d "${matrix[$feed_loc,1]}${DATE}" ];
                then
                	echo "rmdir ${matrix[$feed_loc,1]}${DATE}/*"
                #        rmdir ${matrix[$feed_loc,1]}${DATE}/*
                        echo "rmdir ${matrix[$feed_loc,1]}${DATE}"
                #        rmdir ${matrix[$feed_loc,1]}${DATE}
                fi
		echo "hadoop fs -mkdir -p /FlareData/ReprocessBackup/$FEED"
				#hadoop fs -mkdir /FlareData/ReprocessBackup/$FEED
                echo "hadoop fs -cp  ${matrix[$feed_loc,3]}/tbl_dt=${DATE}  ${HDFSBackupPath}/$FEED/tbl_dt=${DATE}_${currentDate}"
                                        #hadoop fs -cp  ${matrix[$feed_loc,3]}/tbl_dt=${DATE}  /FlareData/backup_before_bib/$FEED/tbl_dt=${DATE}_${currentDate}
                                        #echo "hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${DATE} "
                                        #hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${DATE}
                echo "mv ${FromLocation} ${matrix[$feed_loc,1]}"
                                        #mv ${FromLocation} ${matrix[$feed_loc,1]}
                                        #echo "mv ${matrix[${feed_loc},4]}$DATE /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}${DATE}_${currentDate}"
                                        #mv ${matrix[${feed_loc},4]}$DATE /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}/${DATE}_${currentDate}/

		echo "insert overwrite table  partition (tbl_dt) select * from ${Schema}.${FEED} where file_name not like '%incoming/%${DATE}%' and tbl_dt=${DATE}"
                records_count=$(/opt/presto/bin/presto --server master01004:8099 --catalog hive --schema ${Schema} --output-format  CSV_HEADER --execute  "select count(*) records_count from flare_8.cs5_ccn_voice_da where tbl_dt=20180320")
                echo  "${records_count/$'\n'/=}"
                echo "reprocessing $reprocessingDate for $FEED"
		echo "preparing query to remove duplicates from remaining data"
		remove_duplicates "$FEED" "$Schema"
		#echo "hadoop fs -mkdir  -p  ${commonHdfsPath}_bk/$FEED/tbl_dt=${DATE}_${currentDate}"
		#echo "hadoop fs -mv  ${commonHdfsPath}/$FEED/tbl_dt=${DATE}/Data*-${reprocessingDate}*.dat ${commonHdfsPath}_bk/$FEED/tbl_dt=${DATE}_${currentDate}/"
		#echo "mv  ${FromLocation}/$FEED/${DATE} ${matrix[$feed_loc,1]}"
	fi
fi
#}

#echo ${file_lst[@]} 
