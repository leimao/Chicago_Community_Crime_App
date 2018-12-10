--You have to add jar first before you do anything.
ADD JAR hdfs:///leimao/jars/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar;

--In case Tez does not work
SET hive.execution.engine=mr;

--Join Crime and Weather by dates
--Chicago station number: 725300

CREATE TABLE IF NOT EXISTS leimao_last_one_year_weather (
    DateTimeString string,
    DateTime date,
    Year smallint,
    Month tinyint,
    Day tinyint,
    Temperature double,
    Visibility double,
    Windspeed double,
    Fog boolean,
    Rain boolean,
    Snow boolean,
    Hail boolean,
    Thunder boolean,
    Tornado boolean
    --Clear boolean
) 
STORED AS ORC;

INSERT OVERWRITE TABLE leimao_last_one_year_weather
    SELECT CONCAT(CAST(weather.year AS STRING), LPAD(CAST(weather.month AS STRING), 2, "0"), LPAD(CAST(weather.day AS STRING), 2, "0")), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(CAST(weather.year AS STRING), LPAD(CAST(weather.month AS STRING), 2, "0"), LPAD(CAST(weather.day AS STRING), 2, "0")), "yyyyMMdd"))) AS datetime, weather.year AS year, weather.month AS month, weather.day AS day, weather.meantemperature AS temperature, weather.meanvisibility AS visibility, weather.meanwindspeed AS windspeed, weather.fog AS fog, weather.rain AS rain, weather.snow AS snow, weather.hail AS hail, weather.thunder AS thunder, weather.tornado AS tornado
    FROM leimao_weather weather WHERE TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(CAST(weather.year AS STRING), LPAD(CAST(weather.month AS STRING), 2, "0"), LPAD(CAST(weather.day AS STRING), 2, "0")), "yyyyMMdd"))) >= DATE_SUB(CURRENT_DATE, 365) AND weather.station = 725300;
