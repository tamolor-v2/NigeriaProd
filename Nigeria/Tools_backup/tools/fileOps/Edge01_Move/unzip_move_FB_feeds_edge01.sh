#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/MoveFromlocal_FB.pid
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    exit 1
  fi
fi


date=$(date +"%Y-%m-%d %H:%M:%S")
date_v=$(date +"%Y%m%d")
filesListed=`ls $path | grep zip`
working=/data/data_lz/beegfs/live/CELL_DATA/working/
archived=/nas/share05/archived/CELL_DATA/
feedBasePath=/data/data_lz/beegfs/live
path=/data/data_lz/beegfs/live/CELL_DATA/incoming/
edgeTwo="10.1.197.142"

cd $path
if [ "$(ls -A $path | grep csv)" ]                                         # check if there is no files to be encrypted
then
        FILES=/data/data_lz/beegfs/live/CELL_DATA/incoming/*            # getting the files with the space
        for file_name in $FILES                                         # looping on the files
        do
                echo "INFO: Working on $file_name .."
		rsync -aPEmivvz --remove-source-files $file_name $edgeTwo:$feedBasePath/CELL_DATA/working/csv
		retVal=$?
                if [ $retVal -eq 0 ]                                    # check if the unzipping succeeded
        	then
                        echo "INFO: Done working on $file_name at $date Successfully!"
        	else
                        echo "ERROR: moving $file_name file Failed!"
			exit 1
        	fi

        done
else
  echo "WARN: No files to be moved!"
  exit 1
fi

echo "INFO: Done moving the files!"

