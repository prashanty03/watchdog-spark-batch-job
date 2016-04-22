# Watcgdog-spark-batch-job
Watchdog batch which runs every day to perform the analysis and store the results in cassandra
# Cassandra version required : dsc-cassandra-2.1.11
# Spark-Cassandra-Java connector required : spark-cassandra-connector-java-assembly-1.3.0-SNAPSHOT


#===============================

#Keyspace

CREATE KEYSPACE dog WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

#Schemas for the databases


#=================================


#Master device table 

CREATE TABLE device(device_id text, device_name text, device_type text, channel text, active boolean, user_id text, primary key(device_id,device_type));

#Indexes on device for query purposes


CREATE INDEX on dog.device (device_name);

CREATE INDEX on dog.device (active);

CREATE INDEX on dog.device (user_id);

CREATE INDEX on dog.device (device_type);

CREATE INDEX on dog.device (channel);


#================================

#Table for refrigerator

CREATE TABLE refrigerator(device_id text,device_type text,channel text, date text, time timestamp,temperature double, primary key((device_id,date),time));

#Indexes on refrigerator for query purposes


CREATE INDEX on dog.refrigerator (device_type);

CREATE INDEX on dog.refrigerator (channel);

CREATE INDEX on dog.refrigerator (date);

CREATE INDEX on dog.refrigerator (time);

CREATE INDEX on dog.refrigerator (temperature);


#Table for daily statistic analysis for avg temperature (Refrigerator)


CREATE table dailystatisticsrefrigeratordata(device_id text,date text,dailyaverage double, primary key(device_id,date));

#Indexes on dailystatisticsrefrigeratordata for query purposes


CREATE INDEX on dog.dailystatisticsrefrigeratordata (date);

CREATE INDEX on dog.dailystatisticsrefrigeratordata (dailyaverage);




#Table for daily statistic analysis of all similar device for avg temperature (All Refrigerator)


CREATE table dailystatisticsrefrigeratoralldevice(device_type text, date text, dailyaverageall double, primary key (device_type,date));

#Indexes on dailystatisticsrefrigeratoralldevice for query purposes


CREATE INDEX on dog.dailystatisticsrefrigeratoralldevice (date);

CREATE INDEX on dog.dailystatisticsrefrigeratoralldevice (dailyaverageall);

#==============================

#Table for Television

CREATE TABLE television (device_id text,device_type text,channel text, date text, time timestamp,status int, primary key((device_id,date),time));


#Indexex on Television table for query purposes

CREATE INDEX on dog.television (device_type);

CREATE INDEX on dog.television (channel);

CREATE INDEX on dog.television (date);

CREATE INDEX on dog.television (time);

CREATE INDEX on dog.television (status);


#Table for daily statistic analysis for television data

create table dog.dailystatisticstelevisiondata(device_id text,date text,dailyusage double, primary key(device_id,date));


#Indexex on Television table for query purposes


CREATE INDEX on dog.dailystatisticstelevisiondata (date);

CREATE INDEX on dog.dailystatisticstelevisiondata (dailyusage);


#Table for daily statistic analysis of all similar device for avg usage (All Televisions)

create table dailystatisticstelevisionalldevice(device_type text, date text, dailyusage double, primary key (device_type,date))


#Indexes on dailystatisticstelevisionalldevice for query purposes

CREATE INDEX on dog.dailystatisticstelevisionalldevice (date);

CREATE INDEX on dog.dailystatisticstelevisionalldevice (dailyusage);

#=============================

# SQL table structure for storing average value

CREATE TABLE dailystatisticsdata (
id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
device_id VARCHAR(30) NOT NULL,
date VARCHAR(30) NOT NULL,
averagevalues DOUBLE(10,2)
);


CREATE TABLE dailystatisticsalldevice (
id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
device_type VARCHAR(30) NOT NULL,
date VARCHAR(30) NOT NULL,
averagevalues DOUBLE(10,2)
);

