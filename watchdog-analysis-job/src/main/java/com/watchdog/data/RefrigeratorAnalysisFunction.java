package com.watchdog.data;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import java.time.LocalDate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;



public class RefrigeratorAnalysisFunction {
	


	//Perform initial temperature average analysis for Refrigerators
public static  void performInitialFridgeTempAnalysis(JavaRDD<String> differentDevices,JavaRDD<String> deviceDate, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) {
	   
    for (String differentTv : differentDevices.toArray()) {
    	for (String deviceDataDate : deviceDate.toArray()) {
    	String StatementCheck = "device_id = \'" + differentTv+ "\' AND date = \'"+ deviceDataDate +"\' ";
    	
    	System.out.println(StatementCheck);
    	JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "refrigerator", mapColumnTo(Double.class))
    			.select("temperature")
    			.where(StatementCheck);
    	
    	deviceRow1.toArray().forEach(System.out::println);
    	double avg = 0;
    	for (Double cassandraRow : deviceRow1.toArray()) {
    			avg += cassandraRow;
    	}
    	avg= avg/(deviceRow1.count());
    	System.out.println(avg);
    	
        //if (avg>=0) {
    // Inserting average temperature data for specific devices daily
        statement = session.prepare("INSERT INTO dog.dailystatisticsrefrigeratordata " +
              "(device_id, date, dailyaverage) " +
              "VALUES (?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(differentTv,deviceDataDate,avg));        
        //}
	}
    }
}




//Perform daily temperature average analysis for Refrigerators
public static  void performDailyFridgeTempAnalysis(JavaRDD<String> differentDevices,LocalDate myDateTime, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) {
	String today = myDateTime.toString(); 
    for (String differentTv : differentDevices.toArray()) {
    	
    	String StatementCheck = " device_id = \'" + differentTv+ "\' AND date = \'"+ today +"\' ";
    	System.out.println(StatementCheck);
    	JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "refrigerator", mapColumnTo(Double.class))
    			.select("temperature")
    			.where(StatementCheck);
    	
    	deviceRow1.toArray().forEach(System.out::println);
    	double avg = 0;
    	for (Double cassandraRow : deviceRow1.toArray()) {
    			avg += cassandraRow;
    	}
    	avg= avg/(deviceRow1.count());
    	System.out.println(avg);
    	
        //if (avg>=0) {
        // Inserting average temperature data for specific devices for initial run
        statement = session.prepare("INSERT INTO dog.dailystatisticsrefrigeratordata " +
              "(device_id, date, dailyaverage) " +
              "VALUES (?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(differentTv,today,avg));    
       // }	
	
    }
}




//Perform daily average temperature analysis for all Refrigerators
public static  void performDailyAllFridgeTempAnalysis(JavaRDD<String> differentDevices,LocalDate myDateTime, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) 
{
	String today = myDateTime.toString(); 
	double avg = 0;
	long count =0;
   for (String differentTv : differentDevices.toArray()) {
    	
    	String StatementCheck = "device_id= \'" + differentTv+ "\'  AND date = \'"+ today +"\'";
    	String StatementCheck1 = "device_id= \'" + differentTv+ "\'  AND date = '2016-04-17'";
    	//System.out.println(StatementCheck1);
    	JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "refrigerator", mapColumnTo(Double.class))
    			.select("temperature")
    			.where(StatementCheck1);
    	deviceRow1.toArray().forEach(System.out::println);
    	deviceRow1.toArray().forEach(System.out::println);
    	
    	for (Double cassandraRow : deviceRow1.toArray()) {
    			avg += cassandraRow;
    	}
    	//System.out.println(avg);
    	
    	count = count + deviceRow1.count();
	
    }
   
   avg= avg/(count);
   System.out.println(count);
   System.out.println(avg);
	// Inserting average temperature data for specific devices for initial run
	statement = session.prepare("INSERT INTO dog.dailystatisticsrefrigeratoralldevice " +
		      "(device_type, date, dailyaverageall) " +
		      "VALUES (?, ?, ?);");
	boundStatement = new BoundStatement(statement);
	 
	session.execute(boundStatement.bind(DeviceTypes.REFRIGERATOR.toString(),today,avg));

}




//Perform initial average temperature analysis for all Refrigerators
public static  void performInitalAllFridgeTempAnalysis(JavaRDD<String> differentDevices,JavaRDD<String> deviceDate, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) 
{
 for (String deviceDataDate : deviceDate.toArray()) {
     double avg = 0;
        long count =0;
   for (String differentTv : differentDevices.toArray()) {
        String StatementCheck = "device_id= \'" + differentTv+ "\'  AND date = \'"+ deviceDataDate +"\'";
        //String StatementCheck1 = "device_id= \'" + differentTv+ "\'  AND date = '2016-04-17'";
    System.out.println(StatementCheck);
        JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "refrigerator", mapColumnTo(Double.class))
                .select("temperature")
                .where(StatementCheck);
        deviceRow1.toArray().forEach(System.out::println);
        //deviceRow1.toArray().forEach(System.out::println);
        
        for (Double cassandraRow : deviceRow1.toArray()) {
                avg += cassandraRow;
        }
        //System.out.println(avg);
        
        count = count + deviceRow1.count();
    
    }
   avg= avg/(count);
   System.out.println(count);
    System.out.println(avg);
   statement = session.prepare("INSERT INTO dog.dailystatisticsrefrigeratoralldevice" +
              "(device_type, date, dailyaverageall) " +
              "VALUES (?, ?, ?);");
    boundStatement = new BoundStatement(statement);
    
       
    session.execute(boundStatement.bind(DeviceTypes.REFRIGERATOR.toString(),deviceDataDate,avg));
   }
   
   
    // Inserting average temperature data for specific devices for initial run
    

}


}
