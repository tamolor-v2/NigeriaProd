
currentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
today=$(date  '+%Y%m%d')
ts=`date +'%Y%m%d%H%M%S'`
datetime=`date +'%Y%m%d%H%M%S'`
removeDir=$(date -d '-60 day' '+%Y%m%d')

mkdir -p /mnt/beegfs/tools/incomingNumberCheck/log/${today}
mkdir -p /mnt/beegfs/tools/incomingNumberCheck/tmp/${today}
rm -r /mnt/beegfs/tools/incomingNumberCheck/log/${removeDir}
rm -r /mnt/beegfs/tools/incomingNumberCheck/tmp/${removeDir}

cp ${currentDir}/../config/incomingCheckNumber_template.json ${currentDir}/../tmp/${today}/incomingCheckNumber_${datetime}.json
cp ${currentDir}/../config/log4j2_template.xml ${currentDir}/../tmp/${today}/log4j2_${datetime}.xml
sed -i -e "s/dateTemp/${today}/g" ${currentDir}/../tmp/${today}/log4j2_${datetime}.xml
sed -i -e "s/dateTemp/${today}/g" ${currentDir}/../tmp/${today}/incomingCheckNumber_${datetime}.json


time java -Dlog4j.configurationFile=${currentDir}/../tmp/${today}/log4j2_${datetime}.xml -jar  /mnt/beegfs/tools/incomingNumberCheck/jar/NumberChecker_2.10-1.0.jar  -i -c  ${currentDir}/../tmp/${today}/incomingCheckNumber_${datetime}.json 2>&1 | tee "/mnt/beegfs/tools/incomingNumberCheck/log/${today}/incoming_number_check_${mytime}.log"  
