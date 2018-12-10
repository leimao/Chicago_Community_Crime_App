#!/bin/bash
# Some constants
projectPath=$(pwd)
weatherDataDir="$projectPath/data/weatherData"
crimeDataDir="$projectPath/data/crimeData"
miscellaneousDataDir="$projectPath/data/miscellaneousData"
weatherThriftJar="$projectPath/hdfs/weather_data_ingest/target/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar"
weatherSpeedLayerJar="$projectPath/kafka/speed_layer_weather_update/target/uber-speed_layer_weather_update-0.0.1-SNAPSHOT.jar"
crimeSpeedLayerJar="$projectPath/kafka/speed_layer_crime_update/target/uber-speed_layer_crime_update-0.0.1-SNAPSHOT.jar"
beelineConfigure="jdbc:hive2://localhost:10000"

if [ "$1" == "--download-data" ]
    then
        downloadData=true
    else
        downloadData=false
fi

# Clear data
echo "----------------------------------"
echo "Cleaning existing data ..."
echo "----------------------------------"
hbase shell ./hbase/clearHBase.txt 
#hive -f ./hive/clearHive.hql
beeline -u $beelineConfigure -f ./hive/clearHive.hql
chmod +x ./hdfs/clearHDFS.sh
./hdfs/clearHDFS.sh

# Download data
if $downloadData
    then
        rm -rf $weatherDataDir
        rm -rf $crimeDataDir
        rm -rf $miscellaneousData
        echo "----------------------------------"
        echo "Downloading data ..."
        echo "----------------------------------"
        chmod +x ./hdfs/getWeather.sh
        chmod +x ./hdfs/getCrime.sh
        chmod +x ./miscellaneous/getStations.sh
        chmod +x ./miscellaneous/getCommunity.sh
        ./hdfs/getWeather.sh $weatherDataDir
        ./hdfs/getCrime.sh $crimeDataDir
        ./miscellaneous/getStations.sh $miscellaneousDataDir
        ./miscellaneous/getCommunity.sh $miscellaneousDataDir
fi

# Ingest data into HDFS
echo "----------------------------------"
echo "Ingesting data into HDFS ..."
echo "----------------------------------"
chmod +x ./hdfs/ingestWeather.sh
chmod +x ./hdfs/ingestCrime.sh
chmod +x ./hdfs/ingestMiscellaneous.sh 
./hdfs/ingestWeather.sh $weatherDataDir $weatherThriftJar
./hdfs/ingestCrime.sh $crimeDataDir
./hdfs/ingestMiscellaneous.sh $miscellaneousDataDir

# Load data into Hive
echo "----------------------------------"
echo "Loading data into Hive ..."
echo "----------------------------------"
#hive -f ./hive/loadCrime.hql
#hive -f ./hive/loadWeather.hql
#hive -f ./hive/loadCommunity.hql

beeline -u $beelineConfigure -f ./hive/loadCrime.hql
beeline -u $beelineConfigure -f ./hive/loadWeather.hql
beeline -u $beelineConfigure -f ./hive/loadCommunity.hql

# Prepare Hive Views
echo "----------------------------------"
echo "Preparing Hive views ..."
echo "----------------------------------"
#hive -f ./hive/joinCrimeWeather.hql
#hive -f ./hive/viewCrimeFrequency.hql

beeline -u $beelineConfigure -f ./hive/joinCrimeWeather.hql
beeline -u $beelineConfigure -f ./hive/viewCrimeFrequency.hql
beeline -u $beelineConfigure -f ./hive/viewLastOneYearWeather.hql

# Prepare HBase Views
echo "----------------------------------"
echo "Preparing HBase views ..."
echo "----------------------------------"
hbase shell ./hbase/createHBase.txt 
#hive -f ./hbase/writeHBase.hql

beeline -u $beelineConfigure -f ./hbase/writeHBase.hql

# Test query in Hive
echo "----------------------------------"
echo "Testing query in Hive ..."
echo "----------------------------------"
#hive -f ./hive/testQuery.hql

beeline -u $beelineConfigure -f ./hive/testQuery.hql

# Test query in HBase
echo "----------------------------------"
echo "Testing query in HBase ..."
echo "----------------------------------"
hbase shell ./hbase/testQuery.txt

# Start Kafka Stream
echo "----------------------------------"
echo "Start Kafka Stream ..."
echo "----------------------------------"
chmod +x ./kafka/createTopics.sh
./kafka/createTopics.sh
chmod +x ./kafka/startSpeedLayers.sh
./kafka/startSpeedLayers.sh $weatherSpeedLayerJar $crimeSpeedLayerJar

