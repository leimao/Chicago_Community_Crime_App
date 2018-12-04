#!/bin/bash
# Pass target data directory as argument
if [ -z "$1" ]
    then
        dataDir="../data/crimeData"
    else
        dataDir="$1"
fi

dataAllUrl="https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"
data2017Url="https://data.cityofchicago.org/api/views/d62x-nvdr/rows.csv?accessType=DOWNLOAD"

mkdir -p $dataDir

dataFile2017="chicago-crime-2017.csv"
dataFileAll="chicago-crime-2001-present.csv"

# curl -o $dataDir/$dataFile2017 $data2017Url
curl -o $dataDir/$dataFileAll $dataAllUrl
