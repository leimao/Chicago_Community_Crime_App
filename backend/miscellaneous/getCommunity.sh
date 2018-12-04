#!/bin/bash
if [ -z "$1" ]
    then
        dataDir="../data/miscellaneousData"
    else
        dataDir="$1"
fi

dataCommunity="https://data.cityofchicago.org/api/views/igwz-8jzy/rows.csv?accessType=DOWNLOAD"
dataFile="communityIds.csv"

# Get community number information
mkdir -p $dataDir
curl -o $dataDir/$dataFile $dataCommunity

