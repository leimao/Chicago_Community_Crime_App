#!/bin/bash
# Some constants
projectPath=$(pwd)
crimeDataSimulationJar="$projectPath/kafka/kafka_simulated_crime/target/uber-kafka_simulated_crime-0.0.1-SNAPSHOT.jar"

# Send simulatd crime data to Kafka topic
java -cp $crimeDataSimulationJar edu.uchicago.leimao.kafka_simulated_crime.CrimeArrivals 10.0.0.2:6667

# Check the data is actually been consumed
# ./usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper 10.0.0.2:2181 --topic leimao_crime_update

