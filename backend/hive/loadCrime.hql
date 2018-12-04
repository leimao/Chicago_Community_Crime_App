--Create an ORC table with crime data

--Show tables
--SHOW TABLES;

--Map CSV data in Hive
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_crime_csv (
    ID int,
    CaseNumber string,
    DateTime string,
    Block string,
    IUCR string,
    PrimaryType string,
    Description string,
    LocationDescription string,
    Arrest boolean,
    Domestic boolean,
    Beat string,
    District string,
    Ward smallint,
    CommunityArea smallint,
    FBICode string,
    XCoordinate int,
    YCoordinate int,
    Year smallint,
    UpdatedOn string,
    Latitude double,
    Longitude double,
    Location string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE 
LOCATION '/leimao/inputs/chicagoCrime'
TBLPROPERTIES (
    'skip.header.line.count' = '1');

--Run the following test query to make sure the csv data was loaded in Hive properly.
--SELECT * FROM leimao_crime_csv LIMIT 5;

--Create an ORC table for crime data
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_crime (
    ID int,
    CaseNumber string,
    DateTime string,
    Block string,
    IUCR string,
    PrimaryType string,
    Description string,
    LocationDescription string,
    Arrest boolean,
    Domestic boolean,
    Beat string,
    District string,
    Ward smallint,
    CommunityArea smallint,
    FBICode string,
    XCoordinate int,
    YCoordinate int,
    Year smallint,
    UpdatedOn string,
    Latitude double,
    Longitude double,
    Location string
) 
STORED AS ORC;

--Copy the CSV table to the ORC table
--INSERT OVERWRITE TABLE crime SELECT * FROM crime_csv
--WHERE 
--XCoordinate IS NOT NULL AND
--YCoordinate IS NOT NULL AND
--Latitude IS NOT NULL AND
--Longitude IS NOT NULL AND
--CommunityArea IS NOT NULL AND
--Date IS NOT NULL;
INSERT OVERWRITE TABLE leimao_crime SELECT * FROM leimao_crime_csv;

--Run the following test query to make sure the orc data was loaded in Hive properly.
--SELECT * FROM leimao_crime LIMIT 5;