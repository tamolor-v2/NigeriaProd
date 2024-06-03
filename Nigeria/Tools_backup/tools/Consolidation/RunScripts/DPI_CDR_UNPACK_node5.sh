#!/bin/bash

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds DpiCdrUnpack --startDate 2018-09-11  --endDate 2018-09-30 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/DPI_CDR_UNPACK_5_$(date +%Y%m%d_%s).log

