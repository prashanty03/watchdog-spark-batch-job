package com.watchdog.job;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import com.watchdog.data.RefrigeratorAnalysisFunction;
public class WatchDogAnalysis {

public static void main(String[] args) {
	Cluster cluster;
	Session session;
	PreparedStatement statement = null;
	BoundStatement boundStatement = null;
	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	session = cluster.connect("dog");
	 SparkConf sparkConf = new SparkConf()
             .setAppName("WatchDog Analysis System")
             .set("spark.cassandra.connection.host", "127.0.0.1")
             .setMaster("local[4]");
	 
	 JavaSparkContext sc = new JavaSparkContext(sparkConf);
	 LocalDate locale = LocalDate.now( ZoneId.of( "America/Los_Angeles" ) ) ;
	 System.out.println(locale.toString());
    JavaRDD<String> differentDevices = javaFunctions(sc).cassandraTable("dog", "refrigerator", mapColumnTo(String.class)).select("device_id").distinct();
    differentDevices.toArray().forEach(System.out::println);
    JavaRDD<String> datesForIntialRun = javaFunctions(sc).cassandraTable("dog", "refrigerator", mapColumnTo(String.class)).select("date").distinct();
    //Analysis system for televisions
    JavaRDD<String> differentTelevisionDevices = javaFunctions(sc).cassandraTable("dog", "television", mapColumnTo(String.class)).select("device_id").distinct();
    differentTelevisionDevices.toArray().forEach(System.out::println);
    JavaRDD<String> datesForIntialTelevisionRun = javaFunctions(sc).cassandraTable("dog", "television", mapColumnTo(String.class)).select("date").distinct();
    
    
//    JavaRDD<String> justCheckUser_id = javaFunctions(sc).cassandraTable("dog", "device", mapColumnTo(String.class)).select("user_id")
//    		.where("user_id=?",'1');
//    justCheckUser_id.toArray().forEach(System.out::println);
    
    //Analysis method calls for Refrigerator
    RefrigeratorAnalysisFunction.performInitialFridgeTempAnalysis(differentRefrigeratorDevices, datesForIntialRefrigeratorRun, session, boundStatement, statement, sc);      
    RefrigeratorAnalysisFunction.performDailyFridgeTempAnalysis(differentRefrigeratorDevices, locale, session, boundStatement, statement, sc);
    RefrigeratorAnalysisFunction.performDailyAllFridgeTempAnalysis(differentRefrigeratorDevices,locale, session, boundStatement, statement, sc);

    
    //Analysis method calls for Televisions
    TelevisionAnalysisFunction.performInitialTelevisionUsageAnalysis(differentTelevisionDevices, datesForIntialTelevisionRun, session, boundStatement, statement, sc);
    TelevisionAnalysisFunction.performDailyTelevisionUsageAnalysis(differentTelevisionDevices, locale, session, boundStatement, statement, sc);
    TelevisionAnalysisFunction.performDailyAllelevisionUsageAnalysis(differentTelevisionDevices, locale, session, boundStatement, statement, sc);
cluster.close();
}

}