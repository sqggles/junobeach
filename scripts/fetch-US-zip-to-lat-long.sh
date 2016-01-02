#!/bin/bash

HADOOP=/opt/local/hadoop-2.7.1
HIVE=/opt/local/hive-1.2.1

#CivicSpace US Zipcode database: http://www.boutell.com/zipcodes/
curl -o data/zipcode.zip http://www.boutell.com/zipcodes/zipcode.zip
unzip -d data/zipcode data/zipcode.zip
cat data/zipcode/zipcode.csv | awk '
  BEGIN{
    FS=",";
    OFS=","
  }(NR > 1 && $1 ~ /[0-9]/){
    gsub(/"/,"",$0);
    if ($7 == 1){ $7="TRUE";}else{ $7="FALSE";}
    print
  }' > data/zipcode/zipcode_clean.csv

NUM_ZIPS=`wc -l data/zipcode/zipcode_clean.csv`

echo "Cleaned up zipcodes from CivicSpace DB:$NUM_ZIPS"

HADOOP_HOME=$HADOOP $HADOOP/bin/hadoop --config ~/hdp23 fs -put ~/dev/FPL/data/zipcode/zipcode_clean.csv /tmp/

HADOOP_HOME=$HADOOP $HIVE/bin/beeline < scripts/load-US-zip-data.hql
