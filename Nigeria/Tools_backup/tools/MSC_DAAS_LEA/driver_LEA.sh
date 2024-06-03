#!/bin/bash
processDate=$(date +%Y%m%d)
echo $processDate
bash   /mnt/beegfs/tools/MSC_DAAS_LEA/LEA_test.scala --date $processDate  
