#!/bin/bash

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds SGSN_CDR --startDate 2018-07-21  --endDate 2018-07-31 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/SGSN_CDR_1_$(date +%Y%m%d_%s).log
