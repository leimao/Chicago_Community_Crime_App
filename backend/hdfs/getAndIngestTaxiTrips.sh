#!/bin/bash
# Pass target data directory as argument
if [ -z "$1" ]
    then
        dataDir="../data/taxiTripData"
    else
        dataDir="$1"
fi

dataUrl="https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD"
hdfsTargetDir="/leimao/inputs/chicagoTaxiTrips/"

hdfs dfs -mkdir -p $hdfsTargetDir

curl $dataUrl | hdfs dfs -put - $hdfsTargetDir