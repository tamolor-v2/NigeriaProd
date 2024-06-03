#!/bin/bash

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds CS5_CCN_SMS_AC --startDate 2018-08-01  --endDate 2018-08-25 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/CS5_CCN_SMS_AC_5_$(date +%Y%m%d_%s).log

