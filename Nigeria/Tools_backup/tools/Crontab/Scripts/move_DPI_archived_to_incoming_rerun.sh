#!/bin/bash
yest=$1
incoming_dir=/mnt/beegfs/live/DPI_CDR/incoming/
archived_dir=/mnt/beegfs/production/archived/DPI

echo "mv $archived_dir/$yest $incoming_dir"
mv $archived_dir/$yest $incoming_dir

