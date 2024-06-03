 #!/bin/bash
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] ";}
FEED=
CONF=
DATE=
echo ${dir_lst[@]}
while [[ $# -gt 0 ]]
        do
        case $1 in
        --config)
        CONF=$2
echo "conf=$CONF"
        shift
        ;;
        --feed)
        FEED=$2
	echo "feed=$FEED"
        shift
        ;;
        --date)
	echo "date=$2"
        DATE=$2
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
#echo "$feed_name--->$daas_processing_path----->$bib_path"
        matrix[$i,0]=$feed_name
	matrix[$i,1]=$daas_processing_path
	matrix[$i,2]=$bib_path
	matrix[$i,3]=$hdfs_path
done
for ((i=1;i<index;i++)) do
if [[ ${matrix[$i,0]} == $FEED ]]
then
#echo "-------> ${matrix[$i,0]}"
echo "move $daas_path$DATE $daas_path${DATE}_bk   "
echo "${matrix[$i,2]} ${matrix[$i,1]} "
fo1=/home/daasuser/samer_scripts/reprocess
($(find  ${fo1}  -type f  -exec mv  {} ${fo1}/backup \; ))
#file_lst=($(find  ${matrix[$i,2]} -type f -name "${DATE}*" -exec mv  {} ${matrix[$i,1]} \; ))

#echo ${dir_lst[0]}
fi
done

#echo "MYARRAY is: ${MYARRAY[*]}"
#echo "Total IPs in the file: ${index}"
#echo "$FEED ---->  $DATE"
#source $CONF 
#./reprocess.conf
#echo $MSC
