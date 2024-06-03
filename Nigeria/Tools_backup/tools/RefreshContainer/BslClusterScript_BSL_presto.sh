#!/bin/bash
set -e

# BslClusterScript.sh

export TOOL_LOCATION=/nas/share05/tools/ContainerRefresh
export KAMANJA_HOME=/home/daasuser/FlareBslCluster/FlareBsl
export CLASS_PATH=${TOOL_LOCATION}/lib/ContainersRefresh-1.0.1.jar:${TOOL_LOCATION}/lib/kvinit_2.11-1.5.5.jar:${KAMANJA_HOME}/lib/system/ExtDependencyLibs2_2.11-1.5.5.jar:${KAMANJA_HOME}/lib/system/ExtDependencyLibs_2.11-1.5.5.jar:${KAMANJA_HOME}/lib/system/KamanjaInternalDeps_2.11-1.5.5.jar

java -Dlog4j.configurationFile=${TOOL_LOCATION}/conf/BslClusterLog4j2.xml -cp ${CLASS_PATH} com.ligadata.containersrefresh.App --config ${TOOL_LOCATION}/conf/BslClusterConfig_BSL_presto.json --containers all --jdbcName m8668



