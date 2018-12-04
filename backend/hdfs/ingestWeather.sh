#!/bin/bash
# Pass target data directory as the first argument
# HDFS target directory was set to "/leimao/inputs/thriftWeather/" in the hadoop script.
if [ -z "$1" ]
    then
        dataDir="../data/weatherData"
    else
        dataDir="$1"
fi

# Pass jar path as the second argument
if [ -z "$2" ]
    then
        jarPath="./weather_data_ingest/target/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar"
    else
        jarPath="$2"
fi

# Put a copy of the jar in HDFS
hdfsTargetDir="/leimao/jars/"
hdfs dfs -mkdir -p $hdfsTargetDir
hdfs dfs -put $jarPath $hdfsTargetDir

hadoop jar $jarPath edu.uchicago.leimao.weather_data_ingest.SerializeWeatherSummary $dataDir