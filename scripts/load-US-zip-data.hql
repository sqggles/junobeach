
!connect jdbc:hive2://sandbox.hortonworks.com:10000 hive  org.apache.hive.jdbc.HiveDriver;

create database if not exists loggerhead;

drop table if exists loggerhead.us_zip_to_lat_long;

create table loggerhead.us_zip_to_lat_long ( zip string, city string, state string, latitude float, longitude float, timezone tinyint, dst boolean) row format delimited fields terminated by ',';

load data inpath '/tmp/zipcode_clean.csv' overwrite into table loggerhead.us_zip_to_lat_long;
