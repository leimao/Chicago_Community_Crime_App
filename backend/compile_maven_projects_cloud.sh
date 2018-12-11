#!/bin/bash
# Some constants
projectPath=$(pwd)
weatherThriftDir="$projectPath/hdfs/weather_data_ingest"
weatherSpeedLayerDir="$projectPath/kafka/speed_layer_weather_update_cloud"
crimeSpeedLayerDir="$projectPath/kafka/speed_layer_crime_update_cloud"
crimeDataStreamSimulatorDir="$projectPath/kafka/kafka_simulated_crime"

cd $weatherThriftDir
mvn clean install

cd $weatherSpeedLayerDir
mvn clean install

cd $crimeSpeedLayerDir
mvn clean install

cd $crimeDataStreamSimulatorDir
mvn clean install