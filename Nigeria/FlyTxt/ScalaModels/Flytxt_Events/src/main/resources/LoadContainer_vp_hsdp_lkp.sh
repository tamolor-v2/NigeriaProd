#!/bin/bash
set -e

# LoadClusterScript.sh

export TOOL_LOCATION=/nas/share05/tools/ContainerRefresh
export KAMANJA_HOME=/home/daasuser/FlareCluster/Flare
##export KAMANJA_HOME=/home/daasuser/Kamanja_test

export CLASS_PATH=${KAMANJA_HOME}/lib/system/containersrefresh_2.11.jar:${KAMANJA_HOME}/lib/system/kvinit_2.11.jar:${TOOL_LOCATION}/lib/latest/presto-jdbc-309.jar:${KAMANJA_HOME}/lib/system/dbutils_2.11.jar:${KAMANJA_HOME}/lib/system/ExtDependencyLibs2_2.11.jar:${KAMANJA_HOME}/lib/system/ExtDependencyLibs_2.11.jar:${KAMANJA_HOME}/lib/system/KamanjaInternalDeps_2.11.jar

java -Dlog4j.configurationFile=${TOOL_LOCATION}/conf/LoadClusterLog4j2.xml -cp ${CLASS_PATH} com.ligadata.containersrefresh.App --config ${TOOL_LOCATION}/conf/LoadClusterConfig.json --containers vp_hsdp_lookup --jdbcName m1004 -p 3
~
~
