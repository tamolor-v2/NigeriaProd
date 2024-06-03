#!/bin/bash


kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds MOBILE_MONEY --startDate 2018-07-21  --endDate 2018-07-31 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/MOBILE_MONEY_2_$(date +%Y%m%d_%s).log
