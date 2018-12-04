--You have to add jar first before you do anything.
ADD JAR hdfs:///leimao/jars/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar;

--In case Tez does not work
SET hive.execution.engine=mr;

--Join Crime and Weather by dates
--Chicago station number: 725300

CREATE TABLE IF NOT EXISTS leimao_crime_and_weather (
    ID int,
    CaseNumber string,
    DateTime string,
    PrimaryType string,
    Arrest boolean,
    District string,
    CommunityArea smallint,
    XCoordinate int,
    YCoordinate int,
    Latitude double,
    Longitude double,
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
) 
STORED AS ORC;

INSERT OVERWRITE TABLE leimao_crime_and_weather
    SELECT crime.id AS id, crime.casenumber AS casenumber, crime.datetime AS datetime, crime.primarytype AS primarytype, crime.arrest AS arrest, crime.district AS district, crime.communityarea AS communityarea, crime.xcoordinate AS xcoordinate, crime.ycoordinate AS ycoordinate, crime.latitude AS latitude, crime.longitude AS longitude, weather.year AS year, weather.month AS month, weather.day AS day, weather.meantemperature AS temperature, weather.meanvisibility AS visibility, weather.meanwindspeed AS windspeed, weather.fog AS fog, weather.rain AS rain, weather.snow AS snow, weather.hail AS hail, weather.thunder AS thunder, weather.tornado AS tornado
    FROM leimao_crime crime
    JOIN leimao_weather weather ON 
    INT(SPLIT(SPLIT(crime.datetime, ' ')[0], '/')[2]) = weather.year AND 
    INT(SPLIT(SPLIT(crime.datetime, ' ')[0], '/')[1]) = weather.month AND 
    INT(SPLIT(SPLIT(crime.datetime, ' ')[0], '/')[0]) = weather.day
    WHERE 
    weather.station = 725300 AND
    crime.xcoordinate IS NOT NULL AND
    crime.ycoordinate IS NOT NULL AND
    crime.communityarea IS NOT NULL;