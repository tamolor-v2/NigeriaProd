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

#unzipping the files
date=$(date +"%Y-%m-%d %H:%M:%S")
date_v=$(date +"%Y%m%d")
filesListed=`ls $path | grep zip`
working=/data/data_lz/beegfs/live/CELL_DATA/working/
archived=/nas/share05/archived/CELL_DATA/
feedBasePath=/data/data_lz/beegfs/live
path=/data/data_lz/beegfs/live/CELL_DATA/incoming/
edgeTwo="10.1.197.142"

cd $working
if [ "$(ls -A $path | grep zip)" ]                                         # check if there is no files to be encrypted
then
	SAVEIFS=$IFS
	IFS=$(echo -en "\n\b")
	# set me
	FILES=/data/data_lz/beegfs/live/CELL_DATA/incoming/*		# getting the files with the space
	for file_name in $FILES						# looping on the files
	do
		echo "INFO: Working on $file_name .."
		jar xvf "$file_name"					# unzipping the file
                retVal=$?
		if [ $retVal -eq 0 ]                                    # check if the unzipping succeeded
        then
			echo "INFO: Done working on $file_name at $date Successfully!"
			mkdir /nas/share05/archived/CELL_DATA/$date_v
			echo "INFO: archiving the original file - $file_name"
			mv $file_name /nas/share05/archived/CELL_DATA/$date_v 
			echo "INFO: done archiving the file"
        else
			echo "ERROR: unzipping $file_name file Failed!"
        fi

	done
	# restore $IFS
	IFS=$SAVEIFS
	cd ~
else
  echo "WARN: No .zip files to be unzipped!"
fi

#moving the files to it LZ dirs.

echo "INFO: Moving the files to it LZ"

cd $working/csv
mv Cell_Info_2G* $feedBasePath/CELL_INFO_2G/incoming/
mv Cell_Info_3G* $feedBasePath/CELL_INFO_3G/incoming/
mv Cell_Info_4G* $feedBasePath/CELL_INFO_4G/incoming/
mv Cell_QOS_2G* $feedBasePath/CELL_QOS_2G/incoming/
mv Cell_QOS_3G* $feedBasePath/CELL_QOS_3G/incoming/
mv Cell_QOS_4G* $feedBasePath/CELL_QOS_4G/incoming/
mv 3G\ Daily* $feedBasePath/MTN_NG_3G_DAILY/incoming/
mv 4G\ Daily* $feedBasePath/MTN_NG_4G_DAILY/incoming/
mv 3G\ Monthly* $feedBasePath/MTN_NG_3G_MONTHLY/incoming/
mv 4G\ Monthly* $feedBasePath/MTN_NG_4G_MONTHLY/incoming/
mv Site_Info* $feedBasePath/MAPS_SITE_INFO/incoming/
mv MTNN_Asset_Extract_2G* $feedBasePath/MTNN_ASSET_EXTRACT_2G_CELLS/incoming/
mv MTNN_Asset_Extract_3G* $feedBasePath/MTNN_ASSET_EXTRACT_3G_CELLS/incoming/
mv MTNN_Asset_Extract_4G* $feedBasePath/MTNN_ASSET_EXTRACT_4G_CELLS/incoming/

echo "INFO: Done moving the files!"
#mv $feedBasePath/MTN_NG_GATEWAY_INFORMATION/incoming/
#mv $feedBasePath/TOPOLOGY_MAP/incoming/
#mv $feedBasePath/POPULATION_DS/incoming/

