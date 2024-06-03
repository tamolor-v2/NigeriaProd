#!/bin/bash
#search_dir="/mnt/beegfs/production/live/MOBILE_MONEY/processed/"
search_dir="/home/daasuser/samer_scripts/MOBILE_MONEY/processed/"
file_lst=($(find  $search_dir -type f ))
#echo ${file_lst[@]}
for i in "${file_lst[@]}"
do
#folder="${i:0:5}"
#month="${i:51:2}"
#day="${i:53:2}"
#year="${i:55:4}"
#rest="${i:59:80}"
month="${i:52:2}"
day="${i:54:2}"
year="${i:56:4}"
rest="${i:60:80}"

newFile=$year$month$day$rest
echo "$i  ----------->  $search_dir$newFile"
#mv $i $search_dir$newFile
done
#for entry in "$search_dir"/*
#do
#  echo "$entry"

#done
