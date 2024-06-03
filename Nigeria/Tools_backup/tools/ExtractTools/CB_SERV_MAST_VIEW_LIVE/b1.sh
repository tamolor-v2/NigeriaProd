#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

tbl_name=$1
k=$(echo $tbl_name |tail -c 7)
echo "$k"
