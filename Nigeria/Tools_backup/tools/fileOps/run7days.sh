java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar -in /mnt/beegfs/CDR/CCN_SMS_MA/incoming_7_days -out /mnt/beegfs/CDR/CCN_SMS_MA/ -od -of -tf 50 -nt 30 -iffl ".gz$" -iffl ".txt$" -dp 15 -mv /mnt/beegfs/structured/CDR/CCN_SMS_MA/ -rg "^.{0,10}"

java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar -in /mnt/beegfs/structured/CDR/CCN_SMS_MA/ -out /mnt/beegfs/structured/CDR/CCN_SMS_MA/ -od -of -tf 50 -nt 30 -iffl ".gz$" -iffl ".txt$" -dp 15 -op lineCount

java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar -in /mnt/beegfs/CDR/CCN_GPRS_AC/incoming_7_days -out /mnt/beegfs/CDR/CCN_GPRS_AC/ -od -of -tf 50 -nt 30 -iffl ".gz$" -iffl ".txt$" -dp 15 -mv /mnt/beegfs/structured/CDR/CCN_GPRS_AC/ -rg "^.{0,10}"

java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar -in /mnt/beegfs/structured/CDR/CCN_GPRS_AC/ -out /mnt/beegfs/structured/CDR/CCN_GPRS_AC/ -od -of -tf 50 -nt 30 -iffl ".gz$" -iffl ".txt$" -dp 15 -op lineCount

java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar -in /mnt/beegfs/CDR/CCN_SMS_AC/incoming_7_days -out /mnt/beegfs/CDR/CCN_SMS_AC/ -od -of -tf 50 -nt 30 -iffl ".gz$" -iffl ".txt$" -dp 15 -mv /mnt/beegfs/structured/CDR/CCN_SMS_AC/ -rg "^.{0,10}"

java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar -in /mnt/beegfs/structured/CDR/CCN_SMS_AC/incoming_7_days -out /mnt/beegfs/structured/CDR/CCN_SMS_AC/ -od -of -tf 50 -nt 30 -iffl ".gz$" -iffl ".txt$" -dp 15 -op lineCount
