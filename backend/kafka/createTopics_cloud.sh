
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper class-m-0-20181017030211.us-central1-a.c.mpcs53013-2018.internal:2181 --replication-factor 1 --partitions 1 --topic leimao_crime_update
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper class-m-0-20181017030211.us-central1-a.c.mpcs53013-2018.internal:2181 --replication-factor 1 --partitions 1 --topic leimao_weather_update

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper class-m-0-20181017030211.us-central1-a.c.mpcs53013-2018.internal:2181