#!/bin/bash
if [ -z "$1" ]
    then
        dataDir="../data/miscellaneousData"
    else
        dataDir="$1"
fi

# Get station number information
mkdir -p $dataDir
wget ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt -P $dataDir

