#!bin/bash

declare -a Feeds=("vw_er_subs_base" "vw_er_subs_base2" "vw_er_subs_base1" "vw_er_recharges" "vw_er_revenue" "vw_er_voice_usg" "vw_er_sms_usg" "vw_er_in_voice_traffic" "vw_er_data_traffic" "vw_engine_room_data_rec1" )
declare -a FeedsPos=(0 1 2 3 4 5 6 7 8 9 )
declare -a List

for i in ${FeedsPos[@]}
 do 
  List+=($(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema flare_8 --execute "select count(*) from ${Feeds[$i]}"))
 done

last_tbl=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema flare_8 --execute "select count(*) from tbl_er_vas_bundle_rev")
last_tbl_1=$(echo $last_tbl | sed "s/\"//g")

if [ ! $last_tbl_1 == 0 ]
 then
  if [[ ! ${List[@]} =~ "0" ]]
  then
   exit 0 
  else
   exit 1   
  fi   
else
exit 1
fi
