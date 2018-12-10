
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic leimao_crime_update
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic leimao_weather_update

kafka-topics.sh --list --zookeeper localhost:2181