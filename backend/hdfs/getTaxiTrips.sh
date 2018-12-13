#!/bin/bash
# Pass target data directory as argument
if [ -z "$1" ]
    then
        dataDir="../data/taxiTripData"
    else
        dataDir="$1"
fi

dataUrl="https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD"

mkdir -p $dataDir

dataFile="chicago-taxitrips.csv"

curl -C - -o $dataDir/$dataFile $dataUrl