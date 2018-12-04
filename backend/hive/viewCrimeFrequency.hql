--You have to add jar first before you do anything.
ADD JAR hdfs:///leimao/jars/uber-weather_data_ingest-0.0.1-SNAPSHOT.jar;

--In case Tez does not work
SET hive.execution.engine=mr;

SET hivevar:SCALE_CONST=1;

CREATE TABLE leimao_crime_counts (
    communityId smallint,
    crime_sum bigint,
    fog_crime_sum bigint,
    rain_crime_sum bigint,
    snow_crime_sum bigint,
    hail_crime_sum bigint,
    thunder_crime_sum bigint,
    tornado_crime_sum bigint,
    clear_crime_sum bigint
);


INSERT OVERWRITE TABLE leimao_crime_counts
    SELECT communityarea, COUNT(1), COUNT(IF(fog, 1, NULL)), COUNT(IF(rain, 1, NULL)), COUNT(IF(snow, 1, NULL)), COUNT(IF(hail, 1, NULL)), COUNT(IF(thunder, 1, NULL)), COUNT(IF(tornado, 1, NULL)), COUNT(IF(!fog AND !rain AND !snow AND !hail AND !thunder AND !tornado, 1, null)) FROM leimao_crime_and_weather GROUP BY communityarea;



CREATE TABLE leimao_weather_counts (
    days int,
    fog_days int,
    rain_days int,
    snow_days int,
    hail_days int,
    thunder_days int,
    tornado_days int,
    clear_days int
);

INSERT OVERWRITE TABLE leimao_weather_counts 
    SELECT COUNT(1), COUNT(IF(fog, 1, NULL)), COUNT(IF(rain, 1, NULL)), COUNT(IF(snow, 1, NULL)), COUNT(IF(hail, 1, NULL)), COUNT(IF(thunder, 1, NULL)), COUNT(IF(tornado, 1, NULL)), COUNT(IF(!fog AND !rain AND !snow AND !hail AND !thunder AND !tornado, 1, null)) FROM leimao_weather WHERE station = 725300;


CREATE TABLE leimao_community_crime_counts (
    communityId smallint,
    communityName string,
    communityArea double,
    crime_sum bigint,
    fog_crime_sum bigint,
    rain_crime_sum bigint,
    snow_crime_sum bigint,
    hail_crime_sum bigint,
    thunder_crime_sum bigint,
    tornado_crime_sum bigint,
    clear_crime_sum bigint,
    days int,
    fog_days int,
    rain_days int,
    snow_days int,
    hail_days int,
    thunder_days int,
    tornado_days int,
    clear_days int
);

--Community name use lowercase
INSERT OVERWRITE TABLE leimao_community_crime_counts
    SELECT cc.communityId, LOWER(c.community), c.shapearea, cc.crime_sum, cc.fog_crime_sum, cc.rain_crime_sum, cc.snow_crime_sum, cc.hail_crime_sum, cc.thunder_crime_sum, cc.tornado_crime_sum, cc.clear_crime_sum, wc.days, wc.fog_days, wc.rain_days, wc.snow_days, wc.hail_days, wc.thunder_days, wc.tornado_days, wc.clear_days
    FROM leimao_crime_counts cc JOIN leimao_community c ON cc.communityId = c.AreaNumber CROSS JOIN leimao_weather_counts wc;






--CREATE TABLE leimao_community_crime_counts (
--    communityId smallint,
--    communityName string,
--    communityArea double,
--    crime_sum bigint,
--    fog_crime_sum bigint,
--    rain_crime_sum bigint,
--    snow_crime_sum bigint,
--    hail_crime_sum bigint,
--    thunder_crime_sum bigint,
--    tornado_crime_sum bigint,
--    clear_crime_sum bigint
--);

--INSERT OVERWRITE TABLE community_crime_counts
--    SELECT cc.communityId, c.community, c.shapearea, cc.crime_sum, cc.fog_crime_sum, cc.rain_crime_sum, cc.snow_crime_sum, cc.hail_crime_sum, cc.thunder_crime_sum, cc.tornado_crime_sum, cc.clear_crime_sum FROM crime_counts cc JOIN community c ON cc.communityId = c.AreaNumber;



--CREATE TABLE leimao_crime_normalized_counts (
--    communityId smallint,
--    communityName string,
--    crime_avg double,
--    fog_crime_avg double,
--    rain_crime_avg double,
--    snow_crime_avg double,
--    hail_crime_avg double,
--    thunder_crime_avg double,
--    tornado_crime_avg double,
--    clear_crime_avg double
--);
--INSERT OVERWRITE TABLE crime_normalized_counts
--    SELECT cc.communityId, c.community, cc.crime_sum/(wc.days * c.shapearea)*${hivevar:SCALE_CONST}, cc.fog_crime_sum/(wc.fog_days * c.shapearea)*${hivevar:SCALE_CONST}, cc.rain_crime_sum/(wc.rain_days * c.shapearea)*${hivevar:SCALE_CONST}, cc.snow_crime_sum/(wc.snow_days * c.shapearea)*${hivevar:SCALE_CONST}, cc.hail_crime_sum/(wc.hail_days * c.shapearea)*${hivevar:SCALE_CONST}, cc.thunder_crime_sum/(wc.thunder_days * c.shapearea)*${hivevar:SCALE_CONST}, cc.tornado_crime_sum/(wc.tornado_days * c.shapearea)*${hivevar:SCALE_CONST}, cc.clear_crime_sum/(wc.clear_days * c.shapearea)*${hivevar:SCALE_CONST} FROM crime_counts cc CROSS JOIN weather_counts wc JOIN community c ON cc.communityId = c.AreaNumber;