--Create an ORC table with community data

--Map CSV data in Hive
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_community_csv (
    Geom string,
    Perimeter int,
    Area int,
    ComArea int,
    ComAreaId int,
    AreaNumber smallint,
    Community string,
    AreaNumberDuplicated smallint,
    ShapeArea double,
    ShapeLength double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE 
LOCATION '/leimao/inputs/community'
TBLPROPERTIES (
    'skip.header.line.count' = '1');

--Run the following test query to make sure the csv data was loaded in Hive properly.
--SELECT * FROM leimao_community_csv LIMIT 5;

--Create an ORC table for community data
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_community (
    Geom string,
    Perimeter int,
    Area int,
    ComArea int,
    ComAreaId int,
    AreaNumber smallint,
    Community string,
    AreaNumberDuplicated smallint,
    ShapeArea double,
    ShapeLength double
) 
STORED AS ORC;

--Copy the CSV table to the ORC table
INSERT OVERWRITE TABLE leimao_community SELECT * FROM leimao_community_csv;

--Run the following test query to make sure the orc data was loaded in Hive properly.
--SELECT * FROM leimao_community LIMIT 5;