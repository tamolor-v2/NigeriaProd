#!/bin/bash
export MAILTO="ammar@ligadata.com"
export CONTENT="/mnt/beegfs/yousef/validationCheck/email/ammarmessage"
export SUBJECT="Test This Shit Please?"
(
 echo "Subject: $SUBJECT"
 echo "MIME-Version: 1.0"
 echo "Content-Type: text/html"
 echo "Content-Disposition: inline"
 cat $CONTENT
) | /usr/sbin/sendmail $MAILTO
