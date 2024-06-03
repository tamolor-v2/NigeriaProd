#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$1
rmdir /mnt/beegfs_bsl/FlareData/CDR/NEWREG_BIOUPDT_POOL_DAILY/incoming/$yest
mv /mnt/beegfs_bsl/FlareData/CDR/NEWREG_BIOUPDT_POOL_DAILY/tmp/$yest /mnt/beegfs_bsl/FlareData/CDR/NEWREG_BIOUPDT_POOL_DAILY/incoming/
