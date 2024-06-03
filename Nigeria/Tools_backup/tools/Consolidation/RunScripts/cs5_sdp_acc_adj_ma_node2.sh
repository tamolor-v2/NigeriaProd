#!/bin/bash

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds CS5_SDP_ACC_ADJ_MA --startDate 2018-04-01  --endDate 2018-04-30 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/CS5_SDP_ACC_ADJ_MA_2_$(date +%Y%m%d_%s).log

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds CS5_SDP_ACC_ADJ_MA --startDate 2018-05-01  --endDate 2018-05-31 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/CS5_SDP_ACC_ADJ_MA_2_$(date +%Y%m%d_%s).log

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds CS5_SDP_ACC_ADJ_MA --startDate 2018-06-01  --endDate 2018-06-30 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/CS5_SDP_ACC_ADJ_MA_2_$(date +%Y%m%d_%s).log

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash   /mnt/beegfs/tools/Consolidation/Driver.scala --cfg /mnt/beegfs/tools/Consolidation/config/DriverConfig.cfg --feeds CS5_SDP_ACC_ADJ_MA --startDate 2018-07-01  --endDate 2018-07-20 --workDir /mnt/beegfs/tools/Consolidation/workDir/  2>&1 | tee  /mnt/beegfs/tools/Consolidation/logs/CS5_SDP_ACC_ADJ_MA_2_$(date +%Y%m%d_%s).log

