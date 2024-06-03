#!/bin/bash

cat $1 $2 | /usr/sbin/sendmail -t

