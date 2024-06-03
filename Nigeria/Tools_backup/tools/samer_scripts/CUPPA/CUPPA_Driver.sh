#!/bin/bash
#bash CUPPA_Driver.sh --config c --date 20180601 --command "--ALL"
Usage(){ echo "Usage: [ --config  ] [--command] [--date]";}
currentDate=`date +%Y%m%d%H%M%S`
echo "$currentDate"
checkHive(){
echo "check Hive: $1"

}
checkKamanja(){
echo "checking Kamanja :$1"

}


checkAll(){
echo "Checking All:$1"
}
COMMAND=""
CONF=""
DATE=""

while [[ $# -gt 0 ]]
        do
        case $1 in
        --config)
        CONF=$2
        shift
        ;;
        --date)
        DATE=(${2/,/ })
        shift
        ;;
        --command)
        COMMAND=$2
        shift
        ;;

        *)
        Usage;exit -1
        shift
        ;;
        esac
        shift
done

#echo "$CONF"
#echo "$DATE"
#echo "$COMMAND"
index=0
while read line ; do
        MYARRAY[$index]="$line"
        index=$(($index+1))
done < $CONF
echo "$MYARRAY"

for ((i=1;i<index;i++)) do
        line=${MYARRAY[$i]}
#read -ra feed_details <<< "$line"
IFS=', ' read -r -a feed_details <<< "$line"
#readarray -td ',' feed_details <<<"$line"; declare -p a;
#feed_details=($(echo "$line" | tr ',' '\n'))
        Kamanja_cluster_Name=${feed_details[0]}
        Kamanja_home_path=${feed_details[1]}
        Metadata_file=${feed_details[2]}
        Script_name=${feed_details[3]}
        matrix[$i,0]=$Kmanja_cluster_Name
        matrix[$i,1]=$Kamanja_home_path
        matrix[$i,2]=$Metadata_file
        matrix[$i,3]=$Script_name
echo "${matrix[$i,1]}"
done
for ((i=1;i<index;i++)) do
echo  ${matrix[${i},0]}
done
if [ ${COMMAND^^} == "ALL" ]; then
#echo "Validating All"
checkAll $COMMAND
elif [ ${COMMAND^^} == "HIVE" ];then
checkHive $COMMAND
elif [ ${COMMAND^^} == "KAMANJA" ];then
checkKamanja $COMMAND
fi

