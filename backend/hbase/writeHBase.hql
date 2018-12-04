DROP TABLE IF EXISTS leimao_community_crime_by_weather;

CREATE EXTERNAL TABLE leimao_community_crime_by_weather (
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
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'crime:communityId,:key,crime:communityArea,crime:crime_sum,crime:fog_crime_sum,crime:rain_crime_sum,crime:snow_crime_sum,crime:hail_crime_sum,crime:thunder_crime_sum,crime:tornado_crime_sum,crime:clear_crime_sum,crime:days,crime:fog_days,crime:rain_days,crime:snow_days,crime:hail_days,crime:thunder_days,crime:tornado_days,crime:clear_days')
TBLPROPERTIES ('hbase.table.name' = 'leimao_community_crime_by_weather');


INSERT OVERWRITE TABLE leimao_community_crime_by_weather
    SELECT * FROM leimao_community_crime_counts;
