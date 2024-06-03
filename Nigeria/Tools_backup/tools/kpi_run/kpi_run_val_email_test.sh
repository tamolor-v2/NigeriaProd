#!/bin/bash
. ~/.bash_profile

if [ $# -eq 0 ]
then
    echo "No arguments supplied, pass date of type 20171101"
        exit;
fi

start=`date`
startcalc=`date +%s`
rdate=$1
rdate1=$(date --date="$rdate -2 day" '+%Y%m%d')
rdate2=$(date --date="$rdate -1 day" '+%Y%m%d')

spark-submit --num-executors 82 --executor-cores 4 --executor-memory 40g --conf spark.yarn.executor.memoryOverhead=6048 --driver-memory 16g --conf spark.yarn.driver.memoryOverhead=5024 --class com.mtn.kpi.SubscriberEvent --master yarn ./bsl-assembly-1.1.jar --date $rdate --zookeeper datanode01003.mtn.com:2181 --hdfsURL hdfs://ngdaas  2>&1 | tee /tmp/Run_${rdate}_SubscriberEvent.log

retRGS=$?

spark-submit --queue Q1 --num-executors 40 --executor-cores 4 --executor-memory 32g --driver-memory 16g --class com.mtn.kpi.ChurnSubscriber --master yarn ./bsl-assembly-1.1.jar --date $rdate --zookeeper datanode01003.mtn.com:2181 --hdfsURL hdfs://ngdaas  2>&1 | tee /tmp/Run_${rdate}_ChurnSubscriber.log

retChurn=$?

spark-submit --queue Q1 --num-executors 40 --executor-cores 4 --executor-memory 32g --driver-memory 16g --class com.mtn.kpi.Fact_Usage --master yarn ./bsl-assembly-1.1.jar --date $rdate --zookeeper datanode01003.mtn.com:2181 --hdfsURL hdfs://ngdaas 2>&1 | tee /tmp/Run_${rdate}_Fact_Usage.log

retUsage=$?

end=`date`
endcalc=`date +%s`
runtime=$((endcalc-startcalc))

echo $runtime

hive --hiveconf tez.queue.name=Q1 -e "alter table KPI.FACT_RGS add partition (aggr='daily', tbl_dt='$rdate') location '/user/data/kpi/nigeria/fact/KPI.FACT_RGS/daily/$rdate'; 
alter table KPI.FACT_SUBSCRIBER_MOVEMENT add partition (aggr='daily', tbl_dt='$rdate') location '/user/data/kpi/nigeria/fact/KPI.FACT_SUBSCRIBER_MOVEMENT/daily/$rdate'; 
alter table KPI.FACT_USAGE add partition (aggr='daily', tbl_dt='$rdate') location '/user/data/kpi/nigeria/fact/KPI.FACT_USAGE/daily/$rdate';"

retHive=$?

echo run complete for $rdate

hr=$(($runtime/3600))
min=$(($runtime%3600/60))
sec=$(($runtime%60))
echo $hr $min $sec

if [ "$retRGS" -gt 1 ] || [ "$retChurn" -gt 1 ] || [ "$retUsage" -gt 1 ] || [ "$retHive" -gt 1 ];
then
Status="Failure"
else
Status="Success"
fi

spark-submit --queue Q1 --num-executors 20 --executor-cores 4 --executor-memory 16g --driver-memory 16g --class com.mtn.kpi.KPIValidation --master yarn ./bsl-assembly-1.1.jar --date $rdate --zookeeper datanode01003.mtn.com:2181 --hdfsURL hdfs://ngdaas 2>&1 | tee /tmp/Run_${rdate}_KPIValidation.log

hdfs dfs -rm -r -skipTrash /tmp/dashboard

columnList=" cast(format_number (SUBSCRIBER_RGS_90, 0) as String), cast(format_number (GROSS_ADDITION, 0) as String), cast(format_number (SUBSCRIBER_GROWTH, 4) as String), cast(format_number (MOBILE_CHURN, 0) as String),
cast(format_number (MOBILE_CHURN_RATE, 4) as String), cast(format_number (MOBILE_NET_ADDS, 0) as String), cast(format_number (MOBILE_ACTIVE_DATA_SUBSCRIBER, 0) as String), cast(format_number (MOBILE_DATA_SUBSCRIBER, 0) as String),
cast(format_number (MOBILE_MONEY_SUBSCRIBER, 0) as String), cast(format_number (MOBILE_DIGITAL_SERVICE_SUBSCRIBER, 0) as String), cast(format_number (MOBILE_SMS_SUBSCRIBER, 0) as String),
cast(format_number (NUMBER_SUBSCRIBER, 0) as String), cast(format_number (MONTHLY_MOU_PER_SUBSCRIBER, 4) as String), cast(format_number (INTERCONNECT_INCOMING_MINUTES_PER_SUBSCRIBER, 4) as String),
cast(format_number (DATA_SUBSCRIBER, 0) as String), cast(format_number (DATA_SUBSCRIBERS_RATE, 4) as String), cast(format_number (SMART_PHONE_PENETRATION, 4) as String), 
cast(format_number (MONTHLY_MB_USED_PER_DATA_SUBSCRIBER, 4) as String), cast(format_number (DATA_SUBSCRIBER_GROWTH, 4) as String), cast(format_number (SMS_SUBSCRIBER, 0) as String),
cast(format_number (MOBILE_MONTHLY_SMS_SENT_PER_SUBSCRIBER, 4) as String), cast(format_number (ACTIVE_MOMO, 4) as String), cast(format_number (DEVICE_OF_SMART_PHONE_RATE, 4) as String), cast(format_number (HANDSETS_2G, 4) as String),
cast(format_number (HANDSETS_3G, 4) as String), cast(format_number (HANDSETS_4G, 4) as String), cast(format_number (NUMBER_OF_ENTERPRISE_ACCOUNTS, 0) as String), cast(format_number (NUMBER_OF_ENTERPRISE_SUBSCRIBERS, 0) as String),
cast(format_number (NUMBER_OF_ENTERPRISE_DATA_SUBSCRIBERS, 0) as String), cast(format_number (ENTERPRISE_ACTIVE_DATA_USERS_OF_TOTAL_ENTERPRISE, 4) as String) "

hive --hiveconf tez.queue.name=Q1 -e "ALTER TABLE KPI.DASHBOARD_STATS ADD PARTITION (TBL_DT='$rdate') LOCATION '/user/data/kpi/nigeria/fact/KPI.DASHBOARD_STATS/$rdate';"
hive --hiveconf tez.queue.name=Q1 -e "use kpi; 
INSERT OVERWRITE DIRECTORY '/tmp/dashboard/1/' row format delimited FIELDS TERMINATED BY '|' select $columnList from kpi.dashboard_stats where tbl_dt='$rdate1'; 
INSERT OVERWRITE DIRECTORY '/tmp/dashboard/2/' row format delimited FIELDS TERMINATED BY '|' select $columnList from kpi.dashboard_stats where tbl_dt='$rdate2'; 
INSERT OVERWRITE DIRECTORY '/tmp/dashboard/3/' row format delimited FIELDS TERMINATED BY '|' select $columnList from kpi.dashboard_stats where tbl_dt='$rdate';"

kpiDay1=$(hdfs dfs -cat /tmp/dashboard/1/*)
kpiDay2=$(hdfs dfs -cat /tmp/dashboard/2/*)
kpiDay3=$(hdfs dfs -cat /tmp/dashboard/3/*)

(echo "From: DAAS_Note_NG@edge01002.mtn.com"
echo "To: bmustafa@ligadata.com, saleh@ligadata.com, yulbeh@ligadata.com"
echo "Subject: DAAS_Note_MTN_NG_<KPI Run Completed for $rdate>"
echo "Content-type: text/html"
echo "<table border=1>
      <tr><th>KPI Job Summary</th></tr>
      <tr><td>KPI Report Date</td><td>$rdate</td></tr>
      <tr><td>Start Time</td><td>$start</td></tr>
      <tr><td>End Time</td><td>$end</td></tr>
      <tr><td>Duration(hh:mm:ss)</td><td>$hr:$min:$sec</td></tr>
      <tr><td>Status</td><td>$Status</td></tr>
     </table>"

echo "<table border=1>
      <tr><th>KPI Count Summary</th></tr>
      <tr><th>ID's</th><th>KPI's</th><th>$rdate1</th><th>$rdate2</th><th>$rdate</th></tr>      
	  <tr><td>KPI-000001</td><td>Subscribers (RGS 90)                                                </td><td>"$(cut -d'|' -f1 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f1 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f1 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000002</td><td>Gross Additions                                                     </td><td>"$(cut -d'|' -f2 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f2 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f2 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000003</td><td>Subscriber growth %                                                 </td><td>"$(cut -d'|' -f3 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f3 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f3 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000004</td><td>Mobile - Churn                                                      </td><td>"$(cut -d'|' -f4 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f4 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f4 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000005</td><td>Mobile - Churn % (Rate)                                             </td><td>"$(cut -d'|' -f5 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f5 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f5 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000006</td><td>Mobile - Net Adds                                                   </td><td>"$(cut -d'|' -f6 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f6 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f6 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000007</td><td>Mobile - Active Data Subscribers (Active RGS90)                     </td><td>"$(cut -d'|' -f7 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f7 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f7 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000008</td><td>Mobile - Data Subscribers (Active RGS90)                            </td><td>"$(cut -d'|' -f8 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f8 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f8 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000009</td><td>Mobile Money Subscribers (Active RGS90)                             </td><td>"$(cut -d'|' -f9 <<< $kpiDay1)" </td><td>"$(cut -d'|' -f9 <<< $kpiDay2)" </td><td>"$(cut -d'|' -f9 <<< $kpiDay3)" </td></tr>
      <tr><td>KPI-000010</td><td>Mobile Digital Service Subscribers (Active RGS90)                   </td><td>"$(cut -d'|' -f10 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f10 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f10 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000011</td><td>Mobile - SMS subscribers (Active RGS90)                             </td><td>"$(cut -d'|' -f11 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f11 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f11 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000012</td><td>Number of Subscribers                                               </td><td>"$(cut -d'|' -f12 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f12 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f12 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000013</td><td>Monthly MOU per Subscriber                                          </td><td>"$(cut -d'|' -f13 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f13 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f13 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000014</td><td>Interconnect Incoming Minutes per subscriber                        </td><td>"$(cut -d'|' -f14 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f14 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f14 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000015</td><td>Data Subscribers                                                    </td><td>"$(cut -d'|' -f15 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f15 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f15 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000016</td><td>Data Subscribers as a % Total Subscribers                           </td><td>"$(cut -d'|' -f16 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f16 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f16 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000017</td><td>Smart phone penetration % within MTN subscriber base                </td><td>"$(cut -d'|' -f17 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f17 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f17 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000018</td><td>Monthly MB used per Data Subscriber                                 </td><td>"$(cut -d'|' -f18 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f18 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f18 <<< $kpiDay3)"</td></tr>
	  <tr><td>KPI-000019</td><td>Data Subscribers Growth                                             </td><td>"$(cut -d'|' -f19 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f19 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f19 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000020</td><td>SMS Subscribers                                                     </td><td>"$(cut -d'|' -f20 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f20 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f20 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000021</td><td>Mobile - Monthly SMS sent per Subscriber                            </td><td>"$(cut -d'|' -f21 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f21 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f21 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000022</td><td>Active MoMo Subscribers Growth                                      </td><td>"$(cut -d'|' -f22 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f22 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f22 <<< $kpiDay3)"</td></tr>
	  <tr><td>KPI-000023</td><td>% of device of total that are smart phone                           </td><td>"$(cut -d'|' -f23 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f23 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f23 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000024</td><td>2G % of total handsets                                              </td><td>"$(cut -d'|' -f24 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f24 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f24 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000025</td><td>3G % of total handsets                                              </td><td>"$(cut -d'|' -f25 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f25 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f25 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000026</td><td>4G/ LTE of total handsets                                           </td><td>"$(cut -d'|' -f26 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f26 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f26 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000027</td><td>Number of Enterprise accounts                                       </td><td>"$(cut -d'|' -f27 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f27 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f27 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000028</td><td>Number of Enterprise subscribers                                    </td><td>"$(cut -d'|' -f28 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f28 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f28 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000029</td><td>Number of Enterprise data subscribers                               </td><td>"$(cut -d'|' -f29 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f29 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f29 <<< $kpiDay3)"</td></tr>
	  <tr><td>KPI-000030</td><td>Monthly MOU per Subscriber                                          </td><td>"$(cut -d'|' -f13 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f13 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f13 <<< $kpiDay3)"</td></tr>
	  <tr><td>KPI-000031</td><td>Monthly MB used per Data Subscriber                                 </td><td>"$(cut -d'|' -f16 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f16 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f16 <<< $kpiDay3)"</td></tr>
      <tr><td>KPI-000032</td><td>Enterprise Active data users as a % of Total Enterprise connections </td><td>"$(cut -d'|' -f30 <<< $kpiDay1)"</td><td>"$(cut -d'|' -f30 <<< $kpiDay2)"</td><td>"$(cut -d'|' -f30 <<< $kpiDay3)"</td></tr>
	  </table>"

) | /usr/sbin/sendmail -t

echo "End of Script Successful, mail sent!"

