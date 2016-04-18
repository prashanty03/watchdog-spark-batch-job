# watchdog-spark-batch-job
Watchdog batch which runs every day to perform the analysis and store the results in cassandra

Schemas for the databases

#Master device table 


CREATE TABLE device (device_id text, device_name text, device_type text, channel text, active boolean, user_id text, primary key(device_id,device_type));

#Indexes for query purposes


CREATE INDEX on watchdog.device (device_id);
CREATE INDEX on watchdog.device (device_name);
CREATE INDEX on watchdog.device (active);
CREATE INDEX on watchdog.device (user_id);
CREATE INDEX on watcdog.device (device_name);

#Table for refrigerator


CREATE TABLE refrigerator (device_id text,device_type text,channel text, date text, time timestamp,temperature double, primary key((device_id,date),time));

#Indexes on refrigerator for query purposes


CREATE INDEX on watchdog.refrigerator (device_id);
CREATE INDEX on watchdog.refrigerator (device_type);
CREATE INDEX on watchdog.refrigerator (channel);
CREATE INDEX on watchdog.refrigerator (date);
CREATE INDEX on watchdog.refrigerator (time);
CREATE INDEX on watchdog.refrigerator (temperature);


#Table for daily statistic analysis for avg temperature (Refrigerator)


CREATE table dailystatisticsdata (device_id text,date text,dailyaverage double, primary key(device_id,date));

#Indexes on dailystatisticsdata for query purposes


CREATE INDEX on watchdog.dailystatisticsdata (device_id);
CREATE INDEX on watchdog.dailystatisticsdata (date);
CREATE INDEX on watchdog.dailystatisticsdata (dailyaverage);


#Table for daily statistic analysis of all similar device for avg temperature (All Refrigerator)


CREATE table dailystatisticsalldevice(device_type text, date text, dailyaverageall double, primary key (device_type,date));

#Indexes on dailystatisticsdata for query purposes


CREATE INDEX on watchdog.dailystatisticsdata (device_type);
CREATE INDEX on watchdog.dailystatisticsdata (date);
CREATE INDEX on watchdog.dailystatisticsdata (dailyaverageall);
