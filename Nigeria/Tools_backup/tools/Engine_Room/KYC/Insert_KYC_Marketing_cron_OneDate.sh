#!bin/bash

DATE=$1
DATE_prev=$((DATE-1))
emailReceiver=$(cat  /nas/share05/tools/Crontab/Scripts/email_test.dat)
Schema="flare_8"
BaseFeed="vw_engine_room_data_rec1"
BasePath="/nas/share05/tools/Engine_Room/KYC_Marketing"
declare -a Feeds=("vw_er_subs_base" "vw_er_subs_base2" "vw_er_subs_base1" "vw_er_recharges" "vw_er_revenue" "vw_er_voice_usg" "vw_er_sms_usg" "vw_er_in_voice_traffic" "vw_er_data_traffic" "vw_engine_room_data_rec1" )
declare -a FeedsPos=(0 1 2 3 4 5 6 7 8 9 )
mkdir -p ${BasePath}/logs/${LogsDATE}

ssh edge01002 " echo -e 'CronJob \"Insert_KYC_Marketing_OneDate.sh\" Started at $(date +"%T") , for day: $DATE \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Note_MTN_NG_<Inserting into KYC Marketing Table Started for $DATE at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo -e "Step #1: Dropping the views.. \n Dropping the views started! "
for i in ${FeedsPos[@]}
 do
   /opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "drop view if exists ${Schema}.${Feeds[$i]}"
   echo "drop table ${Feeds[$i]} done."
 done
echo "Deleting table tbl_er_vas_bundle_rev .. "
/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "delete from tbl_er_vas_bundle_rev"
echo -e "Dropping the views finished! \n ================================== \n Step #2: Insert into the base views.. "

 bash ${BasePath}/Insert_BaseViews_KYC_Marketing.sh $DATE $DATE_prev

 echo "Making sure the table hasn't data already.. "
 q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.MARKETING_KYC_TABLE  where tbl_dt=${DATE}") 
 q11=$(echo $q1 |  sed "s/\"//g")
       if [ $q11 == 0 ]
        then
         echo "Start inserting to the KYC table .. "
        else
         echo "Removing the old data for the KYC table.. "
         hdfs dfs -rm -skipTrash /FlareData/output_8/MARKETING_KYC_TABLE/tbl_dt=${DATE}/*
       fi
	 /opt/presto/bin/presto --server master01003:8999 --catalog hive5 --execute "insert into engine_room.MARKETING_KYC_TABLE select * from ${Schema}.${BaseFeed}"
	  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.MARKETING_KYC_TABLE  where tbl_dt=${DATE}")
          q11=$(echo $q1 |  sed "s/\"//g")
        if [ $q11 != 0 ]
	 then 
	    echo "Final Step #3: Inserted successfully $q11 rows into KYC table! "
	    ssh edge01002 " echo -e 'CronJob \"Insert_KYC_Marketing_OneDate.sh\" Finished at $(date +"%T"), with $q11 inserted records into KYC table, for day: $DATE \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Note_MTN_NG_<Inserting into KYC Marketing Table Finished for $DATE at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
	 else
	    echo "Final Step #3: Couldn't Insert into KYC due to some Presto issue or emptiness of the base table, Please rerun me to make sure!"
	    ssh edge01002 " echo -e 'CronJob \"Insert_KYC_Marketing_OneDate.sh\" Failed at $(date +"%T"), for day: $DATE \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Alert_MTN_NG_<Inserting into KYC Marketing Table Failed for $DATE at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
	    exit 1
        fi
       
