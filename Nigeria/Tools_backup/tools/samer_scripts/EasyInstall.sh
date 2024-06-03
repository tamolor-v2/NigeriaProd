#!/bin/bash

set -e

installPath=$1
srcPath=$2
ivyPath=$3
KafkaRootDir=$4

currentKamanjaVersion=1.5.3

ver210=${currentKamanjaVersion}_2.10
ver211=${currentKamanjaVersion}_2.11

if [ ! -d "$installPath" ]; then
        echo "Not valid install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "Not valid src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$ivyPath" ]; then
        echo "Not valid ivy path supplied.  It should be the ivy path for dependency the jars."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$KafkaRootDir" ]; then
        echo "Not valid Kafka path supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

migration2_10libsCopiesFor2_11="false"

installPath=$(echo $installPath | sed 's/[\/]*$//')
srcPath=$(echo $srcPath | sed 's/[\/]*$//')
ivyPath=$(echo $ivyPath | sed 's/[\/]*$//')

# *******************************
# Clean out prior installation
# *******************************
rm -Rf $installPath

# *******************************
# Make the directories as needed for version-2.10
# *******************************
mkdir -p $installPath/Kamanja-$ver210/bin
#mkdir -p $installPath/Kamanja-$ver210/lib
mkdir -p $installPath/Kamanja-$ver210/lib/system
mkdir -p $installPath/Kamanja-$ver210/lib/application
mkdir -p $installPath/Kamanja-$ver210/storage
mkdir -p $installPath/Kamanja-$ver210/logs
mkdir -p $installPath/Kamanja-$ver210/config
mkdir -p $installPath/Kamanja-$ver210/documentation
mkdir -p $installPath/Kamanja-$ver210/output
mkdir -p $installPath/Kamanja-$ver210/workingdir
#mkdir -p $installPath/Kamanja-$ver210/template
mkdir -p $installPath/Kamanja-$ver210/template/config
mkdir -p $installPath/Kamanja-$ver210/template/script
#mkdir -p $installPath/Kamanja-$ver210/input/
mkdir -p $installPath/Kamanja-$ver210/ClusterInstall
mkdir -p $installPath/Kamanja-$ver210/KamanjaUI
mkdir -p $installPath/Kamanja-$ver210/DaaS
mkdir -p $installPath/Kamanja-$ver210/DaaS/input/bin
#new one
#mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/bin
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/data
#mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/config
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/function
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/script
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/type
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/template
#new one



# *******************************
# Make the directories as needed for version-2.11
# *******************************
mkdir -p $installPath/Kamanja-$ver211/bin
#mkdir -p $installPath/Kamanja-$ver211/lib
mkdir -p $installPath/Kamanja-$ver211/lib/system
mkdir -p $installPath/Kamanja-$ver211/lib/application
mkdir -p $installPath/Kamanja-$ver211/storage
mkdir -p $installPath/Kamanja-$ver211/logs
mkdir -p $installPath/Kamanja-$ver211/config
mkdir -p $installPath/Kamanja-$ver211/documentation
mkdir -p $installPath/Kamanja-$ver211/output
mkdir -p $installPath/Kamanja-$ver211/workingdir
#mkdir -p $installPath/Kamanja-$ver211/template
mkdir -p $installPath/Kamanja-$ver211/template/config
mkdir -p $installPath/Kamanja-$ver211/template/script
#mkdir -p $installPath/Kamanja-$ver211/input
mkdir -p $installPath/Kamanja-$ver211/ClusterInstall
mkdir -p $installPath/Kamanja-$ver211/KamanjaUI
mkdir -p $installPath/Kamanja-$ver211/DaaS
mkdir -p $installPath/Kamanja-$ver211/DaaS/input/bin
#new one
#mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/bin
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/data
#mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/config
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/function
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/script
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/type
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/template
#new one


#************************************************************
# Making directories for Kamanja InstallMigrationAndCluster
#************************************************************
# *******************************
# Make the directories as needed for version-2.10
# *******************************
#mkdir -p $installPath/KamanjaInstall-$ver210/bin
#mkdir -p $installPath/KamanjaInstall-$ver210/lib/system
#mkdir -p $installPath/KamanjaInstall-$ver210/lib/application
#mkdir -p $installPath/KamanjaInstall-$ver210/logs
#mkdir -p $installPath/KamanjaInstall-$ver210/config
#mkdir -p $installPath/KamanjaInstall-$ver210/template/config
#mkdir -p $installPath/KamanjaInstall-$ver210/template/script

# *******************************
# Make the directories as needed for version-2.11
# *******************************
#mkdir -p $installPath/KamanjaInstall-$ver211/bin
#mkdir -p $installPath/KamanjaInstall-$ver211/lib/system
#mkdir -p $installPath/KamanjaInstall-$ver211/lib/application
#mkdir -p $installPath/KamanjaInstall-$ver211/logs
#mkdir -p $installPath/KamanjaInstall-$ver211/config
#mkdir -p $installPath/KamanjaInstall-$ver211/template/config
#mkdir -p $installPath/KamanjaInstall-$ver211/template/script

kamanjaui=$installPath/Kamanja-$ver210/KamanjaUI
kamanjainstallbin=$installPath/Kamanja-$ver210/ClusterInstall
kamanjainstallconfig=$installPath/Kamanja-$ver210/ClusterInstall


# *******************************
# Build fat-jars for version-2.10
# *******************************

bin=$installPath/Kamanja-$ver210/bin
systemlib=$installPath/Kamanja-$ver210/lib/system
applib=$installPath/Kamanja-$ver210/lib/application

echo $installPath
echo $srcPath
echo $bin

echo "clean, package and assemble $srcPath ..."

cd $srcPath/

sbt clean '++ 2.10.4 package' '++ 2.10.4 ExtDependencyLibs/assembly' '++ 2.10.4 ExtDependencyLibs2/assembly' '++ 2.10.4 KamanjaInternalDeps/assembly' 
sbt '++ 2.10.4 ClusterInstallerDriver/assembly' '++ 2.10.4 GetComponent/assembly' '++ 2.10.4 InstallDriver/assembly'
#   '++ 2.10.4 NodeInfoExtract/assembly' '++ 2.10.4 MigrateManager/assembly'

#sbt clean '++ 2.10.4 package' '++ 2.10.4 KamanjaManager/assembly' '++ 2.10.4 MetadataAPI/assembly' '++ 2.10.4 KVInit/assembly' '++ 2.10.4 SimpleKafkaProducer/assembly'
#sbt '++ 2.10.4 NodeInfoExtract/assembly' '++ 2.10.4 MetadataAPIService/assembly' '++ 2.10.4 JdbcDataCollector/assembly'
#sbt '++ 2.10.4 FileDataConsumer/assembly' '++ 2.10.4 CleanUtil/assembly' '++ 2.10.4 MigrateManager/assembly' '++ 2.10.4 ClusterInstallerDriver/assembly' '++ 2.10.4 InstallDriver/assembly' '++ 2.10.4 GetComponent/assembly' '++ 2.10.4 PmmlTestTool/assembly' '++ 2.10.4 ExtDependencyLibs/assembly' '++ 2.10.4 ExtDependencyLibs2/assembly' '++ 2.10.4 KamanjaInternalDeps/assembly'
# sbt '++ 2.10.4 MethodExtractor/assembly' '++ 2.10.4 SaveContainerDataComponent/assembly' '++ 2.10.4 ExtractData/assembly'

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

cd $srcPath
cp Utils/KVInit/target/scala-2.10/kvinit* $systemlib
cp Utils/ContainersUtility/target/scala-2.10/containersutility* $systemlib
cp MetadataAPI/target/scala-2.10/metadataapi* $systemlib
cp KamanjaManager/target/scala-2.10/kamanjamanager* $systemlib
cp Utils/SaveContainerDataComponent/target/scala-2.10/savecontainerdatacomponent_2.10* $systemlib
cp Utils/MdTool/target/scala-2.10/mdtool* $systemlib


# cp Pmml/MethodExtractor/target/scala-2.10/methodextractor* $bin

cp Utils/SimpleKafkaProducer/target/scala-2.10/simplekafkaproducer* $systemlib
cp InputOutputAdapters/KafkaAdapters_v8/target/scala-2.10/kamanjakafkaadapters* $systemlib
cp InputOutputAdapters/KafkaAdapters_v9/target/scala-2.10/kamanjakafkaadapters* $systemlib
cp InputOutputAdapters/KafkaAdapters_v10/target/scala-2.10/kamanjakafkaadapters* $systemlib

cp Utils/ExtractData/target/scala-2.10/extractdata* $systemlib
cp Utils/JdbcDataCollector/target/scala-2.10/jdbcdatacollector* $systemlib
cp MetadataAPIService/target/scala-2.10/metadataapiservice* $systemlib
cp FileDataConsumer/target/scala-2.10/filedataconsumer* $systemlib
cp Utils/CleanUtil/target/scala-2.10/cleanutil* $systemlib
cp Utils/ClusterInstaller/ClusterInstallerDriver/target/ClusterInstallerDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/target/scala-2.10/InstallDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/GetComponent/target/scala-2.10/GetComponent* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/src/main/resources/GetComponentsVersions.sh $kamanjainstallbin
cp Utils/PmmlTestTool/target/pmmltesttool* $systemlib
cp Utils/JsonChecker/target/scala-2.10/jsonchecker* $systemlib
cp Utils/QueryGenerator/target/scala-2.10/querygenerator* $systemlib
cp Utils/GenerateMessage/target/scala-2.10/generatemessage* $systemlib
cp $ivyPath/cache/org.apache.commons/commons-pool2/jars/commons-pool2-2.4.2.jar $systemlib

# copy fat jars to KamanjaInstall
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/nodeinfoextract* $systemlib

# copy jars used to reduce package size
cp ExtDependencyLibs/target/scala-2.10/ExtDependencyLibs_2.10-${currentKamanjaVersion}.jar $systemlib
cp ExtDependencyLibs2/target/scala-2.10/ExtDependencyLibs2_2.10-${currentKamanjaVersion}.jar $systemlib
cp KamanjaInternalDeps/target/scala-2.10/KamanjaInternalDeps_2.10-${currentKamanjaVersion}.jar $systemlib

# Copy jars needed for Kafka
cp $ivyPath/cache/org.apache.kafka/kafka_2.10/jars/kafka_2.10-0.8.2.2.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.9.0.2.4.2.12-1.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.10.2.1.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.8.2.2.jar $systemlib
cp $ivyPath/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar  $systemlib


# *******************************
# Copy jars required for version-2.10 (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things

#echo "copy all Kamanja jars and the jars upon which they depend to the $systemlib"
#
## -------------------- generated cp commands --------------------
#
cp $srcPath/FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory/target/scala-2.10/jarfactoryofmodelinstancefactory*.jar $systemlib

# -------------------- end of generated cp commands --------------------


###### For Version-2.10 ######
#

cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4/target/scala-2.10/migratefrom_v_1_4_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4_1/target/scala-2.10/migratefrom_v_1_4_1_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_5_0/target/scala-2.10/migrateto_v_1_5_0_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/GenerateAdapterBindings/target/scala-2.10/generateadapterbindings_2.10-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/MigrateManager/target/migratemanager-${currentKamanjaVersion}.jar $systemlib

cp InputOutputAdapters/OracleOutputAdapter/target/scala-2.10/oracleoutputadapter_*.jar $systemlib
cp InputOutputAdapters/OracleOutputAdapter/src/main/resources/ojdbc6.jar $systemlib

#copy jars for kamanjainstallapplib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4/target/scala-2.10/migratefrom_v_1_4_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4_1/target/scala-2.10/migratefrom_v_1_4_1_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_5_0/target/scala-2.10/migrateto_v_1_5_0_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system
cp $srcPath/Utils/Migrate/GenerateAdapterBindings/target/scala-2.10/generateadapterbindings_2.10-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver211/lib/system

migration2_10libsCopiesFor2_11="true"


# sample configs
#echo "copy sample configs..."
# cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# Generate keystore file
#echo "generating keystore..."
#keytool -genkey -keyalg RSA -alias selfsigned -keystore $installPath/config/keystore.jks -storepass password -validity 360 -keysize 2048

#copy kamanja to bin directory
cp $srcPath/Utils/Script/scala-2.10/kamanja $bin
#cp $srcPath/Utils/Script/MedicalApp.sh $bin
cp $srcPath/MetadataAPI/target/scala-2.10/classes/HelpMenu.txt $installPath/Kamanja-$ver210/input
# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
# *******************************
# Copy documentation files
# *******************************
cd $srcPath/Documentation
cp -rf * $installPath/Kamanja-$ver210/documentation

# *******************************
# Copy ClusterInstall
# *******************************
# mkdir -p $installPath/Kamanja-$ver210/ClusterInstall
# cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/* $installPath/Kamanja-$ver210/ClusterInstall/
# cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/NodeInfoExtract* $installPath/Kamanja-$ver210/ClusterInstall/
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*.sh $kamanjainstallbin
cp -rf $srcPath/MetadataAPI/src/main/resources/daas/* $installPath/Kamanja-$ver210/DaaS/
cd $srcPath/MetadataAPI/src/main/resources/daas/input/metadata/template/script/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/DaaS/input/metadata/template/

#Moving StartKamanjaCluster.sh, StatusKamanjaCluster.sh & StopKamanjaCluster.sh to bin
mv $kamanjainstallbin/StartKamanjaCluster.sh $bin
mv $kamanjainstallbin/StatusKamanjaCluster.sh $bin
mv $kamanjainstallbin/StopKamanjaCluster.sh $bin

cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*log4*.xml $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*.json $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*.properties $kamanjainstallconfig
cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/src/main/resources/log4j2.xml $kamanjainstallconfig

# *******************************
# copy Kamanja UI stuff
# *******************************

cp $srcPath/KamanjaUI/Rest/KamanjaUIRest/target/kamanjauirest-${currentKamanjaVersion}.war $kamanjaui/kamanjauirest.war
cp $srcPath/KamanjaUI/UI/distro/kamanja.war $kamanjaui/
cp $srcPath/KamanjaUI/Scripts/* $kamanjaui/
chmod 0700 $kamanjaui/*.sh


# *******************************
# copy OrientDB JDBC jar into system
# *******************************
# Download only once and copy
orientdb_jdbc_all_path="$ivyPath/cache/com.orientechnologies/orientdb-jdbc/jars"
orientdb_jdbc_all="$orientdb_jdbc_all_path/orientdb-jdbc-2.1.19-all.jar"
if [ ! -f "$orientdb_jdbc_all" ]; then
	mkdir -p $orientdb_jdbc_all_path
	wget -O $orientdb_jdbc_all --no-cookies --no-check-certificate "http://orientdb.com/download.php?file=orientdb-jdbc-2.1.19-all.jar"
fi
cp $orientdb_jdbc_all $systemlib

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/template/script

#HelloWorld
cd $srcPath/SampleApplication/HelloWorld/data
cp * $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/HelloWorld/message
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/HelloWorld/model
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/HelloWorld/template
cp -rf conf* $installPath/Kamanja-$ver210/input/SampleApplications/template

cd $srcPath/SampleApplication/HelloWorld/template/script/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template/script

cd $srcPath/SampleApplication/HelloWorld/config
cp -rf * $installPath/Kamanja-$ver210/config
#HelloWorld

#Medical
cd $srcPath/SampleApplication/Medical/SampleData
cp *.csv $installPath/Kamanja-$ver210/input/SampleApplications/data
cp *.csv.gz $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/Medical/template/script/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template/script

cd $srcPath/SampleApplication/Medical/Configs
cp -rf * $installPath/Kamanja-$ver210/config
#Medical

#Telecom
cd $srcPath/SampleApplication/Telecom/data
cp * $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/Telecom/metadata/container
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Telecom/metadata/message
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Telecom/metadata/model
cp *.* $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Telecom/metadata/template
cp -rf conf* $installPath/Kamanja-$ver210/input/SampleApplications/template

cd $srcPath/SampleApplication/Telecom/metadata/template/script/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template/script

cd $srcPath/SampleApplication/Telecom/metadata/config
cp -rf * $installPath/Kamanja-$ver210/config
#Telecom

#Finance
cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/data
cp * $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/container
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/message
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/model
cp *.* $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/type
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template/script/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template/script

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/config
cp -rf * $installPath/Kamanja-$ver210/config
#Finance

cd $srcPath/SampleApplication/EasyInstall/template/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/Kamanja-$ver210/bin/

bash $installPath/Kamanja-$ver210/bin/SetPaths.sh $KafkaRootDir

chmod 0700 $installPath/Kamanja-$ver210/input/SampleApplications/bin/*sh
chmod 0700 $installPath/Kamanja-$ver210/ClusterInstall/*sh

################################ Version-2.10 Finished ################################



# *******************************
# Build fat-jars for version-2.11
# *******************************

echo "clean, package and assemble $srcPath ..."

bin=$installPath/Kamanja-$ver211/bin
systemlib=$installPath/Kamanja-$ver211/lib/system
applib=$installPath/Kamanja-$ver211/lib/application

kamanjaui=$installPath/Kamanja-$ver211/KamanjaUI
kamanjainstallbin=$installPath/Kamanja-$ver211/ClusterInstall
kamanjainstallconfig=$installPath/Kamanja-$ver211/ClusterInstall


echo $installPath
echo $srcPath
echo $bin

# Once we get all 2.10 libraries and copy them to corresponding directories, we can run 2.11 again and copy them to corresponding directories
# sbt clean '++ 2.11.7 package' '++ 2.11.7 KamanjaManager/assembly' '++ 2.11.7 MetadataAPI/assembly' '++ 2.11.7 KVInit/assembly' '++ 2.11.7 MethodExtractor/assembly' '++ 2.11.7 SimpleKafkaProducer/assembly' '++ 2.11.7 NodeInfoExtract/assembly' '++ 2.11.7 ExtractData/assembly' '++ 2.11.7 MetadataAPIService/assembly' '++ 2.11.7 JdbcDataCollector/assembly' '++ 2.11.7 FileDataConsumer/assembly' '++ 2.11.7 SaveContainerDataComponent/assembly' '++ 2.11.7 CleanUtil/assembly' '++ 2.11.7 MigrateManager/assembly'

cd $srcPath
#Build and copy 2.10 for both MigrateFrom_V_1_1 & MigrateFrom_V_1_2, if they are not copied from 2.10.4 build
if [ "$migration2_10libsCopiesFor2_11" == "false" ]; then
	sbt clean '++ 2.10.4 MigrateFrom_V_1_1/package' '++ 2.10.4 MigrateFrom_V_1_2/package' '++ 2.10.4 MigrateFrom_V_1_3/package' '++ 2.10.4 MigrateFrom_V_1_4/package' '++ 2.10.4 MigrateFrom_V_1_4_1/package' '++ 2.10.4 MigrateTo_V_1_5_0/package'
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-${currentKamanjaVersion}.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-${currentKamanjaVersion}.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-${currentKamanjaVersion}.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4/target/scala-2.10/migratefrom_v_1_4_2.10-${currentKamanjaVersion}.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4_1/target/scala-2.10/migratefrom_v_1_4_1_2.10-${currentKamanjaVersion}.jar $systemlib
	cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_5_0/target/scala-2.10/migrateto_v_1_5_0_2.10-${currentKamanjaVersion}.jar $systemlib
fi

#Now do full build of 2.11
sbt clean '++ 2.11.7 package' '++ 2.11.7 ExtDependencyLibs/assembly' '++ 2.11.7 ExtDependencyLibs2/assembly' '++ 2.11.7 KamanjaInternalDeps/assembly' 
sbt '++ 2.11.7 ClusterInstallerDriver/assembly' '++ 2.11.7 GetComponent/assembly' '++ 2.11.7 InstallDriver/assembly' 
#'++ 2.11.7 NodeInfoExtract/assembly' '++ 2.11.7 MigrateManager/assembly'

#sbt clean '++ 2.11.7 package' '++ 2.11.7 KamanjaManager/assembly' '++ 2.11.7 MetadataAPI/assembly' '++ 2.11.7 KVInit/assembly' '++ 2.11.7 SimpleKafkaProducer/assembly'
#sbt '++ 2.11.7 NodeInfoExtract/assembly' '++ 2.11.7 MetadataAPIService/assembly' '++ 2.11.7 JdbcDataCollector/assembly'
#sbt '++ 2.11.7 FileDataConsumer/assembly' '++ 2.11.7 CleanUtil/assembly' '++ 2.11.7 MigrateManager/assembly' '++ 2.11.7 ClusterInstallerDriver/assembly' '++ 2.11.7 InstallDriver/assembly' '++ 2.11.7 GetComponent/assembly' '++ 2.11.7 PmmlTestTool/assembly' '++ 2.11.7 ExtDependencyLibs/assembly' '++ 2.11.7 ExtDependencyLibs2/assembly' '++ 2.11.7 KamanjaInternalDeps/assembly'
# sbt '++ 2.11.7 MethodExtractor/assembly' '++ 2.11.7 SaveContainerDataComponent/assembly' '++ 2.11.7 ExtractData/assembly' 

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

cd $srcPath
cp Utils/KVInit/target/scala-2.11/kvinit* $systemlib
cp Utils/ContainersUtility/target/scala-2.11/containersutility* $systemlib
cp MetadataAPI/target/scala-2.11/metadataapi* $systemlib
cp KamanjaManager/target/scala-2.11/kamanjamanager* $systemlib
cp Utils/SaveContainerDataComponent/target/scala-2.11/savecontainerdatacomponent_2.11* $systemlib
cp Utils/MdTool/target/scala-2.11/mdtool* $systemlib

cp Utils/SimpleKafkaProducer/target/scala-2.11/simplekafkaproducer* $systemlib
cp InputOutputAdapters/KafkaAdapters_v8/target/scala-2.11/kamanjakafkaadapters* $systemlib
cp InputOutputAdapters/KafkaAdapters_v9/target/scala-2.11/kamanjakafkaadapters* $systemlib
cp InputOutputAdapters/KafkaAdapters_v10/target/scala-2.11/kamanjakafkaadapters* $systemlib

cp Utils/ExtractData/target/scala-2.11/extractdata* $systemlib
cp Utils/JdbcDataCollector/target/scala-2.11/jdbcdatacollector* $systemlib
cp MetadataAPIService/target/scala-2.11/metadataapiservice* $systemlib
cp FileDataConsumer/target/scala-2.11/filedataconsumer* $systemlib
cp Utils/CleanUtil/target/scala-2.11/cleanutil* $systemlib
cp Utils/ClusterInstaller/ClusterInstallerDriver/target/ClusterInstallerDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/target/scala-2.11/InstallDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/GetComponent/target/scala-2.11/GetComponent* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/src/main/resources/GetComponentsVersions.sh $kamanjainstallbin
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/nodeinfoextract* $systemlib
cp Utils/PmmlTestTool/target/pmmltesttool* $systemlib
cp Utils/JsonChecker/target/scala-2.11/jsonchecker* $systemlib
cp Utils/QueryGenerator/target/scala-2.11/querygenerator* $systemlib
cp Utils/GenerateMessage/target/scala-2.11/generatemessage* $systemlib
cp $ivyPath/cache/org.apache.commons/commons-pool2/jars/commons-pool2-2.4.2.jar $systemlib

# copy jars used to reduce package size
cp ExtDependencyLibs/target/scala-2.11/ExtDependencyLibs_2.11-${currentKamanjaVersion}.jar $systemlib
cp ExtDependencyLibs2/target/scala-2.11/ExtDependencyLibs2_2.11-${currentKamanjaVersion}.jar $systemlib
cp KamanjaInternalDeps/target/scala-2.11/KamanjaInternalDeps_2.11-${currentKamanjaVersion}.jar $systemlib

cp $ivyPath/cache/org.apache.kafka/kafka_2.11/jars/kafka_2.11-0.8.2.2.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.9.0.2.4.2.12-1.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.8.2.2.jar $systemlib
cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.10.2.1.jar $systemlib
cp $ivyPath/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar  $systemlib


# *******************************
# Copy jars required version-2.11 (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things
echo "copy all Kamanja jars and the jars upon which they depend to the $systemlib"

# -------------------- generated cp commands --------------------

cp $srcPath/FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory/target/scala-2.11/jarfactoryofmodelinstancefactory*.jar $systemlib
# -------------------- end of generated cp commands --------------------


###### For Version-2.11 ######
#

cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.11/migratefrom_v_1_3_2.11-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4/target/scala-2.11/migratefrom_v_1_4_2.11-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4_1/target/scala-2.11/migratefrom_v_1_4_1_2.11-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_5_0/target/scala-2.11/migrateto_v_1_5_0_2.11-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/GenerateAdapterBindings/target/scala-2.11/generateadapterbindings_2.11-${currentKamanjaVersion}.jar $systemlib
cp $srcPath/Utils/Migrate/MigrateManager/target/migratemanager-${currentKamanjaVersion}.jar $systemlib

# copy 2.11 migrate libraries into 2.10 install directories, useful just in case of reverse migration
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.11/migratefrom_v_1_3_2.11-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver210/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4/target/scala-2.11/migratefrom_v_1_4_2.11-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver210/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_4_1/target/scala-2.11/migratefrom_v_1_4_1_2.11-${currentKamanjaVersion}.jar $installPath/Kamanja-$ver210/lib/system/

cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/target/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/InstallDriver/target/scala-2.11/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/InstallDriverBase/target/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/GetComponent/target/scala-2.11/*.jar $systemlib

cp InputOutputAdapters/OracleOutputAdapter/target/scala-2.11/oracleoutputadapter_*.jar $systemlib
cp InputOutputAdapters/OracleOutputAdapter/src/main/resources/ojdbc6.jar $systemlib

# cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.11/SaveContainerDataComponent* $systemlib
#cp $srcPath/Utils/UtilsForModels/target/scala-2.11/utilsformodels*.jar $systemlib

# sample configs
#echo "copy sample configs..."
# cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# Generate keystore file
#echo "generating keystore..."
#keytool -genkey -keyalg RSA -alias selfsigned -keystore $installPath/config/keystore.jks -storepass password -validity 360 -keysize 2048

#copy kamanja to bin directory
cp $srcPath/Utils/Script/scala-2.11/kamanja $bin
cp $srcPath/MetadataAPI/target/scala-2.11/classes/HelpMenu.txt $installPath/Kamanja-$ver211/input
# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
# *******************************
# Copy documentation files
# *******************************
cd $srcPath/Documentation
cp -rf * $installPath/Kamanja-$ver211/documentation

# *******************************
# Copy ClusterInstall
# *******************************
# mkdir -p $installPath/Kamanja-$ver211/ClusterInstall
# cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/* $installPath/Kamanja-$ver211/ClusterInstall/
# cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/NodeInfoExtract* $installPath/Kamanja-$ver211/ClusterInstall/
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*.sh $kamanjainstallbin
cp -rf $srcPath/MetadataAPI/src/main/resources/daas/* $installPath/Kamanja-$ver211/DaaS/
cd $srcPath/MetadataAPI/src/main/resources/daas/input/metadata/template/script/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/DaaS/input/metadata/template/

#Moving StartKamanjaCluster.sh, StatusKamanjaCluster.sh & StopKamanjaCluster.sh to bin
mv $kamanjainstallbin/StartKamanjaCluster.sh $bin
mv $kamanjainstallbin/StatusKamanjaCluster.sh $bin
mv $kamanjainstallbin/StopKamanjaCluster.sh $bin

cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*log4*.xml $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*.json $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*.properties $kamanjainstallconfig
cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/src/main/resources/log4j2.xml $kamanjainstallconfig

# *******************************
# copy Kamanja UI stuff
# *******************************

cp $srcPath/KamanjaUI/Rest/KamanjaUIRest/target/kamanjauirest-${currentKamanjaVersion}.war $kamanjaui/kamanjauirest.war
cp $srcPath/KamanjaUI/UI/distro/kamanja.war $kamanjaui/
cp $srcPath/KamanjaUI/Scripts/* $kamanjaui/
chmod 0700 $kamanjaui/*.sh

# *******************************
# copy OrientDB JDBC jar into system
# *******************************
# Download only once and copy
orientdb_jdbc_all_path="$ivyPath/cache/com.orientechnologies/orientdb-jdbc/jars"
orientdb_jdbc_all="$orientdb_jdbc_all_path/orientdb-jdbc-2.1.19-all.jar"
if [ ! -f "$orientdb_jdbc_all" ]; then
	mkdir -p $orientdb_jdbc_all_path
	wget -O $orientdb_jdbc_all --no-cookies --no-check-certificate "http://orientdb.com/download.php?file=orientdb-jdbc-2.1.19-all.jar"
fi
cp $orientdb_jdbc_all $systemlib

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/template/script

#HelloWorld
cd $srcPath/SampleApplication/HelloWorld/data
cp * $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/HelloWorld/message
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/HelloWorld/model
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/HelloWorld/template
cp -rf conf* $installPath/Kamanja-$ver211/input/SampleApplications/template

cd $srcPath/SampleApplication/HelloWorld/template/script/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template/script

cd $srcPath/SampleApplication/HelloWorld/config
cp -rf * $installPath/Kamanja-$ver211/config
#HelloWorld

#Medical
cd $srcPath/SampleApplication/Medical/SampleData
cp *.csv $installPath/Kamanja-$ver211/input/SampleApplications/data
cp *.csv.gz $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/Medical/template/script/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template/script

cd $srcPath/SampleApplication/Medical/Configs
cp -rf * $installPath/Kamanja-$ver211/config
#Medical

#Telecom
cd $srcPath/SampleApplication/Telecom/data
cp * $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/Telecom/metadata/container
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Telecom/metadata/message
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Telecom/metadata/model
cp *.* $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Telecom/metadata/template
cp -rf conf* $installPath/Kamanja-$ver211/input/SampleApplications/template

cd $srcPath/SampleApplication/Telecom/metadata/template/script/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template/script

cd $srcPath/SampleApplication/Telecom/metadata/config
cp -rf * $installPath/Kamanja-$ver211/config
#Telecom

#Finance
cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/data
cp * $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/container
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/message
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/model
cp *.* $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/type
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template/script/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template/script

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/config
cp -rf * $installPath/Kamanja-$ver211/config
#Finance

cd $srcPath/SampleApplication/EasyInstall/template/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/Kamanja-$ver211/bin/

bash $installPath/Kamanja-$ver211/bin/SetPaths.sh $KafkaRootDir

chmod 0700 $installPath/Kamanja-$ver211/input/SampleApplications/bin/*sh
chmod 0700 $installPath/Kamanja-$ver211/ClusterInstall/*sh

#Migration and cluster Install*****************

#chmod 0700 $installPath/KamanjaInstall-$ver210/bin/*.sh
#chmod 0700 $installPath/KamanjaInstall-$ver211/bin/*.sh

#Migration and Cluster Install*****************

cd $installPath
tar -cvzf Kamanja-$ver210.tar.gz Kamanja-$ver210
tar -cvzf Kamanja-$ver211.tar.gz Kamanja-$ver211
#tar -cvzf KamanjaInstall-$ver210.tar.gz KamanjaInstall-$ver210
#tar -cvzf KamanjaInstall-$ver211.tar.gz KamanjaInstall-$ver211

echo "Kamanja install complete..."

