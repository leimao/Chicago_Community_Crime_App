#!/bin/bash
# Pass target data directory as argument
if [ -z "$1" ]
    then
        dataDir="../data/taxiTripData"
    else
        dataDir="$1"
fi

hdfsTargetDir="/leimao/inputs/chicagoTaxiTrips/"

hdfs dfs -mkdir -p $hdfsTargetDir
for f in $dataDir/*.csv
do
    hdfs dfs -put $f $hdfsTargetDir
done
