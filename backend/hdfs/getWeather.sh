#!/bin/bash
# Pass target data directory as argument
if [ -z "$1" ]
    then
        dataDir="../data/weatherData"
    else
        dataDir="$1"
fi

mkdir -p $dataDir
# cd ./data/weatherData
# Change year here to add more data
# 2001 is an appropriate starting year
year=2017
while [ $year -le 2017 ]
do
    wget ftp://ftp.ncdc.noaa.gov/pub/data/gsod/$year/gsod_$year.tar -P $dataDir
    (( year++ ))
done

for f in $dataDir/*.tar;
do
  tar xf $f -C $dataDir
  rm $f
done
