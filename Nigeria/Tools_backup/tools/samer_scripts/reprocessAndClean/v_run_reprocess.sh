 #!/bin/bash
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] [--hdfs]";}
currentDate=`date +%Y%m%d%H%M%S`
echo "$currentDate"
FEED=
CONF=
DATE=
HDFS_BACK=
FromLocation=
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
        --hdfs)
        HDFS_BACK=$2
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
	echo "hadoop fs -mkdir -p /FlareData/backup_before_bib/$FEED"
	hadoop fs -mkdir -p /FlareData/backup_before_bib/$FEED
	echo "mkdir -p /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}"
	mkdir -p /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}
#	echo "${matrix[$feed_loc,4]} =======> ${matrix[$feed_loc,5]}"
                for dt in "${DATE[@]}"
                do
#        echo "mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}$dt_${currentDate}"
                                    #    echo "hadoop fs -mv  ${matrix[$feed_loc,3]}/tbl_dt=${dt}  /FlareData/backup_before_bib/$FEED/tbl_dt=${dt}_${currentDate}"
                                   #     echo "hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${dt} "
                                  #      echo "mv ${matrix[$feed_loc,2]}${dt} ${matrix[$feed_loc,1]}"
                                 #       echo "mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}${dt}_${currentDate}"

			        #exit

                        echo "Processing $FEED for date: $dt"
			currentDate=`date +%Y%m%d%H%M%S`
                        spool_file_count=($(find  ${matrix[$feed_loc,2]}${dt} -type f -name "${dt}*" |wc -l))
                        processed_file_count=($(find  ${matrix[$feed_loc,6]}${dt} -type f -name "${dt}*" |wc -l))
			if [[ $spool_file_count >0 && $processed_file_count>0 ]]
                        then
                                echo "spool_file_count: $spool_file_count, processed_file_count: $processed_file_count"
                                echo "Files exist in spool and processed, Exitting ......"
				exit
                        fi
                        FromLocation=""

                        if [  -d "${matrix[$feed_loc,6]}${dt}" ]
                        then
                                FromLocation=${matrix[$feed_loc,6]}${dt}
                                echo "$FromLocation exists. taking it"
                        fi


                        if [  -d "${matrix[$feed_loc,2]}${dt}" ]
                        then
                                FromLocation=${matrix[$feed_loc,2]}${dt}
                                echo "$FromLocation exists. taking it"
                        fi
			
                        echo "Sources from $FromLocation"
                        if [  -d "${FromLocation}" ]
                        then
				echo "source directory exist, starting to reprocess...."
				if [  -d "${matrix[$feed_loc,1]}${dt}" ]; 
				then
					echo "rmdir ${matrix[$feed_loc,1]}${dt}/*"
					rmdir ${matrix[$feed_loc,1]}${dt}/*
					echo "rmdir ${matrix[$feed_loc,1]}${dt}"
					rmdir ${matrix[$feed_loc,1]}${dt}
				fi

                        	if $(hadoop fs -test -d  /FlareData/backup_before_bib/$FEED/tbl_dt=${dt})
                        	then
                                	echo "Partition exists in backup folder, will be written with  current run datetime"
                                	echo "hadoop fs -mv  ${matrix[$feed_loc,3]}/tbl_dt=${dt}  /FlareData/backup_before_bib/$FEED/tbl_dt=${dt}_${currentDate}"
                                	hadoop fs -mv  ${matrix[$feed_loc,3]}/tbl_dt=${dt}  /FlareData/backup_before_bib/$FEED/tbl_dt=${dt}_${currentDate}
					echo "hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${dt} "
					hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${dt}
                                	echo "mv ${FromLocation} ${matrix[$feed_loc,1]}"
                                	mv ${FromLocation} ${matrix[$feed_loc,1]}
	                        	echo "mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}${dt}_${currentDate}"
	                        	mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}/${dt}_${currentDate}/
				else
                                	echo "Partition doesn't exist in backup and will be moved as is"
                                	echo "hadoop fs -mv  ${matrix[$feed_loc,3]}/tbl_dt=${dt}  /FlareData/backup_before_bib/$FEED/"
                                	hadoop fs -mv  ${matrix[$feed_loc,3]}/tbl_dt=${dt}  /FlareData/backup_before_bib/$FEED/
                                        echo "hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${dt} "
                                        hadoop fs -mkdir  ${matrix[$feed_loc,3]}/tbl_dt=${dt}
                                        echo "mv ${FromLocation} ${matrix[$feed_loc,1]}"
                                        mv ${FromLocation} ${matrix[$feed_loc,1]}
                                        echo "mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}$dt_${currentDate}"
                                        mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}${dt}_${currentDate}/
                        	fi
                        else
                                echo "Feed $FEED : Couldn't find source directory ${FromLocation} skipping re-process "
                        fi
			echo "--------------------------------------------------------------------"
                        #file_lst=($(find  ${matrix[$feed_loc,2]} -type f -name "${dt}*" |wc -l))
                        #echo  "$FEED has ${file_lst} files that start with date ${dt}"
                        #($(find  ${matrix[$i,2]} -type f -name "${DATE}*" -exec mv  {} ${matrix[$i,1]} \; ))
                done

	#mkdir -p /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}
                        #file_lst=($(find  ${matrix[$feed_loc,2]} -type f -name "${dt}*" |wc -l))
                        #echo  "$FEED has ${file_lst} files that start with date ${dt}"
			#($(find  ${matrix[$i,2]} -type f -name "${DATE}*" -exec mv  {} ${matrix[$i,1]} \; ))


#fo1=/home/daasuser/samer_scripts/reprocess
#($(find  ${fo1} -maxdepth 1  -type f -name "*.sh" -exec cp  {} ${fo1}/backup \; ))
#($(find  ${fo1} -maxdepth 1  -type f -name "*.sh" -exec cp  {} ${fo1}/backup \; ))
#file_lst=($(find  ${matrix[$i,2]} -type f -name "${DATE}*" -exec mv  {} ${matrix[$i,1]} \; ))

#echo ${dir_lst[0]}
	else 
		distinct_dates=()
		file_lst=($(find  ${matrix[$feed_loc,2]} -type f  ))
		for file in "${file_lst[@]}"
		do
			IFS='/' read -r -a file_split <<< "${file}"
			distinct_dates+=(${file_split[-1]:0:8})
		done
		sorted_unique_ids=($(echo "${distinct_dates[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))
		echo "found following dates in $FEED BIB Folder  [${sorted_unique_ids[@]}]"
		echo "hadoop fs -mkdir -p /FlareData/backup_before_bib/$FEED"
		echo "mkdir -p /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}"

        	for dt in "${sorted_unique_ids[@]}"
	        do
	                echo $dt
                        file_lst=($(find  ${matrix[$feed_loc,2]} -type f -name "${dt}*" |wc -l))
                        echo  "$FEED has ${file_lst} files that start with date ${dt}"
	                echo "mv ${matrix[${feed_loc},4]}$dt /mnt/beegfs/production/archived_bk/${matrix[$feed_loc,5]}"
        	        #echo "mv $daas_path$dt $daas_path${dt}_bk_${currentDate}   "
	                echo "hadoop fs -mv  ${matrix[$feed_loc,3]}/tbl_dt=${dt}  /FlareData/backup_before_bib/$FEED/"
			echo "mv ${matrix[$feed_loc,2]}${dt}* ${matrix[$feed_loc,1]}"
                	#($(find  ${matrix[$i,2]} -type f -name "${DATE}*" -exec mv  {} ${matrix[$i,1]} \; ))
        	done

	fi
else
	echo "Feed Name wasn't found in config file"
	exit -1
fi
#distinct_dates=()
#file_lst=($(find  ${matrix[$feed_loc,2]} -type f  ))
#for file in "${file_lst[@]}"
#do

#IFS='/' read -r -a file_split <<< "${file}"
#distinct_dates+=(${file_split[-1]:0:8})
#done
#sorted_unique_ids=($(echo "${distinct_dates[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))
#echo "${sorted_unique_ids[@]}"
#echo "MYARRAY is: ${MYARRAY[*]}"
#echo "Total IPs in the file: ${index}"
#echo "$FEED ---->  $DATE"
#source $CONF 
#./reprocess.conf
#echo $MSC
