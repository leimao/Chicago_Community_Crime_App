
CREATE EXTERNAL TABLE leimao_community_crime_by_weather (
    communityName string,
    communityId smallint,
    --communityId bigint,
    --communityName string,
    communityArea double,
    --communityArea bigint,
    crime_sum bigint,
    fog_crime_sum bigint,
    rain_crime_sum bigint,
    snow_crime_sum bigint,
    hail_crime_sum bigint,
    thunder_crime_sum bigint,
    tornado_crime_sum bigint,
    clear_crime_sum bigint,
    --days int,
    --fog_days int,
    --rain_days int,
    --snow_days int,
    --hail_days int,
    --thunder_days int,
    --tornado_days int,
    --clear_days int
    days bigint,
    fog_days bigint,
    rain_days bigint,
    snow_days bigint,
    hail_days bigint,
    thunder_days bigint,
    tornado_days bigint,
    clear_days bigint
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
--Immutable Cell
--WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'crime:communityId,:key,crime:communityArea,crime:crime_sum,crime:fog_crime_sum,crime:rain_crime_sum,crime:snow_crime_sum,crime:hail_crime_sum,crime:thunder_crime_sum,crime:tornado_crime_sum,crime:clear_crime_sum,crime:days,crime:fog_days,crime:rain_days,crime:snow_days,crime:hail_days,crime:thunder_days,crime:tornado_days,crime:clear_days')
--Incrementable Cell
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,crime:communityId,crime:communityArea,crime:crime_sum#b,crime:fog_crime_sum#b,crime:rain_crime_sum#b,crime:snow_crime_sum#b,crime:hail_crime_sum#b,crime:thunder_crime_sum#b,crime:tornado_crime_sum#b,crime:clear_crime_sum#b,crime:days#b,crime:fog_days#b,crime:rain_days#b,crime:snow_days#b,crime:hail_days#b,crime:thunder_days#b,crime:tornado_days#b,crime:clear_days#b')
TBLPROPERTIES ('hbase.table.name' = 'leimao_community_crime_by_weather');

INSERT OVERWRITE TABLE leimao_community_crime_by_weather
    SELECT * FROM leimao_community_crime_counts;


CREATE EXTERNAL TABLE leimao_last_one_year_weather_chicago (
    dateTimeString string,
    dateTime date,
    year smallint,
    month tinyint,
    day tinyint,
    temperature double,
    visibility double,
    windspeed double,
    fog boolean,
    rain boolean,
    snow boolean,
    hail boolean,
    thunder boolean,
    tornado boolean
    --clear boolean
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, weather:dateTime, weather:year, weather:month, weather:day, weather:temperature, weather:visibility, weather:windspeed, weather:fog, weather:rain, weather:snow, weather:hail, weather:thunder, weather:tornado')
TBLPROPERTIES ('hbase.table.name' = 'leimao_last_one_year_weather_chicago');

INSERT OVERWRITE TABLE leimao_last_one_year_weather_chicago
    SELECT * FROM leimao_last_one_year_weather;


CREATE EXTERNAL TABLE leimao_taxi_trips_by_community_chicago (
    route string,
    trip_duration_sum bigint,
    total_cost_sum bigint,
    num_trips bigint
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
--Incrementable Cell
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, taxi:trip_duration_sum#b, taxi:total_cost_sum#b, taxi:num_trips#b')
TBLPROPERTIES ('hbase.table.name' = 'leimao_taxi_trips_by_community_chicago');

INSERT OVERWRITE TABLE leimao_taxi_trips_by_community_chicago
    SELECT CONCAT(LOWER(c1.Community), '-', LOWER(c2.Community)), tc.trip_duration_sum, tc.total_cost_sum, tc.num_trips FROM leimao_taxitrips_counts tc JOIN leimao_community c1 ON tc.pickup_community = c1.AreaNumber JOIN leimao_community c2 ON tc.dropoff_community = c2.AreaNumber;
