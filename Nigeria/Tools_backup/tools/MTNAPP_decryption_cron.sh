#!bin/bash

path="/mnt/beegfs_api/live/MTNAPPNXG/incoming"		   # where the encrypted files are
outPath="/mnt/beegfs_api/live/MTNAPPNXG_NEW_DAILY_USERS/incoming" #  where to decrypt the files
origPath="/mnt/beegfs_api/production/live/MTNAPPNXG_NEW_DAILY_USERS/orig_files" # where to keep the original files
failedPath="/mnt/beegfs_api/production/live/MTNAPPNXG_NEW_DAILY_USERS/failed_orig_files" # where to keep the failed original files
filesListed=`ls $path` 					   # list the files at the path 
date=$(date +"%Y-%m-%d %H:%M:%S")			   # get the system date

if [ "$(ls -A $path)" ]				           # check if there is no files to be encrypted		
then
  for i in $filesListed                                   # loop on th files
  do
    echo "Start working on file - $path/$i at $date"
    python3.6 /nas/share05/tools/MTNAPP_dec.py -i "$path/$i" -o "$outPath/$i" -k /nas/share05/tools/mymtn-nextgen-nigeria-sivkey.txt
    retVal=$?
    if [ $retVal -eq 0 ]                                  # check if the encryption succeeded
    then
       echo "INFO: Done working on file - $path/$i at $date Successfully!"
       mv $path/$i $origPath
    else
       echo "ERROR: Failed while working on file - $i !"
       mv $path/$i $failedPath
       #exit 1;
    fi
  done
else
  echo "WARN: No files to be encrypted!"
fi

