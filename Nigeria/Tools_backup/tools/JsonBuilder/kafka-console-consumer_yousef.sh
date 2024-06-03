#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M"
fi

# check if kafka_jaas.conf in config , only enable client_kerberos_params in secure mode.
KAFKA_HOME="$(dirname $(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd ))"
KAFKA_JAAS_CONF=$KAFKA_HOME/config/kafka_jaas.conf
if [ -f $KAFKA_JAAS_CONF ]; then
    export KAFKA_CLIENT_KERBEROS_PARAMS="-Djava.security.auth.login.config=$KAFKA_JAAS_CONF"
fi


exec /usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.ConsoleConsumer "$@"
