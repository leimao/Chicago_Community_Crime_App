#!/bin/bash
# Pass target data directory as argument
if [ -z "$1" ]
    then
        dataDir="../data/miscellaneousData"
    else
        dataDir="$1"
fi

hdfsCommunityTargetDir="/leimao/inputs/community/"

hdfs dfs -mkdir -p $hdfsCommunityTargetDir
hdfs dfs -put $dataDir/communityIds.csv $hdfsCommunityTargetDir

#for f in $dataDir/*.csv
#do
#    hdfs dfs -put $f $hdfsTargetDir
#done
