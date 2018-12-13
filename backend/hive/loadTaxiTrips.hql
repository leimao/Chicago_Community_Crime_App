--Create an ORC table with taxi trips data

--Map CSV data in Hive
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_taxitrips_csv (
    tripID string,
    taxiID string,
    tripStartTime string,
    tripEndTime string,
    tripDuration int,
    tripMiles double,
    pickupCensus string,
    dropoffCensus string,
    pickupCommunity smallint,
    dropoffCommunity smallint,
    fare string,
    tips string,
    tolls string,
    extras string,
    tripTotal string,
    paymentType string,
    company string,
    pickupLatitude double,
    pickupLongitude double,
    pickupLocation string,
    dropoffLatitude double,
    dropoffLongitude double,
    dropoffLocation string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE 
LOCATION '/leimao/inputs/chicagoTaxiTrips'
TBLPROPERTIES (
    'skip.header.line.count' = '1');

--Run the following test query to make sure the csv data was loaded in Hive properly.
--SELECT * FROM leimao_taxitrips_csv LIMIT 5;

--Create an ORC table for crime data
CREATE EXTERNAL TABLE IF NOT EXISTS leimao_taxitrips (
    tripID string,
    taxiID string,
    tripStartTime string,
    tripEndTime string,
    tripDuration int,
    tripMiles double,
    pickupCensus string,
    dropoffCensus string,
    pickupCommunity smallint,
    dropoffCommunity smallint,
    fare string,
    tips string,
    tolls string,
    extras string,
    tripTotal string,
    paymentType string,
    company string,
    pickupLatitude double,
    pickupLongitude double,
    pickupLocation string,
    dropoffLatitude double,
    dropoffLongitude double,
    dropoffLocation string
) 
STORED AS ORC;

INSERT OVERWRITE TABLE leimao_taxitrips SELECT * FROM leimao_taxitrips_csv;

--Run the following test query to make sure the orc data was loaded in Hive properly.
--SELECT * FROM leimao_taxitrips LIMIT 5;