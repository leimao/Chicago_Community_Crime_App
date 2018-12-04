--You have to add jar first before you do anything.
ADD JAR hdfs:///leimao/jars/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar;

--Show table headers
SET hive.cli.print.header=true;

--Run the following test query to make sure the data was loaded in Hive properly.
SELECT * FROM leimao_weather LIMIT 5;
SELECT * FROM leimao_crime LIMIT 5;
SELECT * FROM leimao_community LIMIT 1;
SELECT * FROM leimao_community_crime_counts LIMIT 5;