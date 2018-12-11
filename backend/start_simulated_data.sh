#!/bin/bash
# Some constants
projectPath=$(pwd)
crimeDataSimulationJar="$projectPath/kafka/kafka_simulated_crime/target/uber-kafka_simulated_crime-0.0.1-SNAPSHOT.jar"

# Send simulatd crime data to Kafka topic
java -cp $crimeDataSimulationJar edu.uchicago.leimao.kafka_simulated_crime.CrimeArrivals localhost:9092

# Check the data is actually been consumed
# kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic leimao_crime_update --from-beginning