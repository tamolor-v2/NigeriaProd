WorkingDir=/data01/infa/PROD/sms_alert/smpp/
cd $WorkingDir

if [ $1 "==" "stop" ]
then
clear
echo "##################################################################################"
echo "################################# STOPPING #######################################"
echo "############################## Created by Arome ##################################"
echo ""

rm -f /data01/infa/PROD/sms_alert/smpp/kill_this.sh

##################################################################################
#Stop smpp_send.sh 
##################################################################################
echo "Stopping SMPP SEND.sh..."
cd /data01/infa/PROD/sms_alert/smpp
 
checkrun=`ps -fu pbt1 | grep smpp_send.sh | grep -v "grep" | awk '{print $9}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
if [ $checkrun -eq 0 ]
then
echo "smpp_send.sh is not running"
else
ps -fu pbt1 | grep smpp_send.sh | grep -v "grep" | awk '{print "kill -9 "$2}' >> /data01/infa/PROD/sms_alert/smpp/kill_this.sh

fi 

##################################################################################
#Stop restart_smpp_new.sh
##################################################################################
#echo "Stopping restart_smpp_new.sh..."
#cd /data01/infa/PROD/sms_alert/smpp
 
#checkrun=`ps -fu pbt1 | grep restart_smpp_new.sh | grep -v "grep" | awk '{print $9}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
#if [ $checkrun -eq 0 ]
#then
#echo "restart_smpp_new.sh is not running"
#else
#ps -fu pbt1 | grep restart_smpp_new.sh | grep -v "grep" | awk '{print "kill -9 "$2}' >> /data01/infa/PROD/sms_alert/smpp/kill_this.sh

#fi 

##################################################################################
#Stop restart_smpp.sh
##################################################################################
echo "Stopping restart_smpp.sh..."
cd /data01/infa/PROD/sms_alert/smpp
 
checkrun=`ps -fu pbt1 | grep restart_smpp.sh | grep -v "grep" | awk '{print $9}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
if [ $checkrun -eq 0 ]
then
echo "restart_smpp.sh is not running"
else
ps -fu pbt1 | grep restart_smpp.sh | grep -v "grep" | awk '{print "kill -9 "$2}' >> /data01/infa/PROD/sms_alert/smpp/kill_this.sh

fi 

##################################################################################
#Stop the SMS Watcher 
##################################################################################
echo "Stopping sms_send_watcher.sh..."
cd /data01/infa/PROD/sms_alert/smpp
 
checkrun=`ps -fu pbt1 | grep sms_send_watcher.sh | grep -v "grep" | awk '{print $9}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
if [ $checkrun -eq 0 ]
then
echo "SMS Watcher is not running"
else
ps -fu pbt1 | grep sms_send_watcher.sh | grep -v "grep" | awk '{print "kill -9 "$2}' >> /data01/infa/PROD/sms_alert/smpp/kill_this.sh

fi 

chmod 777 /data01/infa/PROD/sms_alert/smpp/kill_this.sh

bash /data01/infa/PROD/sms_alert/smpp/kill_this.sh

echo "##################################################################################"

else
##################################################################################
#Startup procedure
##################################################################################
clear
echo "##################################################################################"
echo "################################# STARTING #######################################"
echo "############################# Created by Arome ####################################"
echo ""

##################################################################################
#Start the restart_smpp.sh
##################################################################################
echo "Starting restart_smpp..."
checkrun=`ps -fu pbt1 | grep restart_smpp.sh | grep -v "grep" | awk '{print $10}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
if [ $checkrun -eq 0 ]
then
cd /data01/infa/PROD/sms_alert/smpp
nohup bash restart_smpp.sh > /dev/null 2>&1 & 
else
echo "Already running"
fi
sleep 5

##################################################################################
#Start the restart_smpp_new.sh
##################################################################################
#echo "Starting restart_smpp_new..."
#checkrun=`ps -fu pbt1 | grep restart_smpp_new.sh | grep -v "grep" | awk '{print $10}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
#if [ $checkrun -eq 0 ]
#then
#cd /data01/infa/PROD/sms_alert/smpp
#nohup bash restart_smpp_new.sh > /dev/null 2>&1 & 
#else
#echo "Already running"
#fi
#sleep 5

##################################################################################
#Start the smpp_send.sh
##################################################################################
echo "Starting  smpp_send.sh..."
checkrun=`ps -fu pbt1 | grep smpp_send.sh | grep -v "grep" | awk '{print $10}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
if [ $checkrun -eq 0 ]
then
cd /data01/infa/PROD/sms_alert/smpp
nohup bash smpp_send.sh > /dev/null 2>&1 & 
else
echo "Already running"
fi
sleep 5

##################################################################################
#Start the sms_send_watcher.sh
##################################################################################
echo "Starting SMS Watcher..."
checkrun=`ps -fu pbt1 | grep sms_send_watcher.sh | grep -v "grep" | awk '{print $9}' | sort -u | wc -l | sed 's/^[ \t]*//;s/[ \t]*$//'`
if [ $checkrun -eq 0 ]
then
 cd /data01/infa/PROD/sms_alert/smpp
 nohup bash sms_send_watcher.sh > /dev/null 2>&1 &
 echo "SMS Watcher started successfully"
else
echo "SMS Watcher Already running"
fi

fi

exit