--You have to add jar first before you do anything.
--Might have to use ordinary jar instead of uber jar to prevent collisions
ADD JAR hdfs:///leimao/jars/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar;


--'serialization.class' = 'edu.uchicago.mpcs53013.weatherSummary.WeatherSummary',
--'serialization.class' = 'edu.uchicago.mpcs53013.weatherSummary.WeatherSummary',
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_weather
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
    'serialization.class' = 'edu.uchicago.mpcs53013.weatherSummary.WeatherSummary',
    'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol')
STORED AS SEQUENCEFILE
LOCATION '/leimao/inputs/thriftWeather';


--Run the following test query to make sure the thrift data was loaded in Hive properly.
--SELECT * FROM leimao_weather LIMIT 5;
