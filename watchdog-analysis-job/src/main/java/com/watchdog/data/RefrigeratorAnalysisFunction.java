package com.watchdog.analysis;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.watchdog.model.DeviceTypes;
import com.watchdog.analysis.DBConnection;
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

        
            // Inserting average temperature data for specific devices daily
            double meanAvg = 0;
            System.out.println(deviceRow1.count());
            meanAvg = deviceRow1.mapToDouble(x->x).mean();
            if (Double.isNaN(meanAvg)) {
                meanAvg =0;
            }
            java.sql.Connection conn = DBConnection.getInstance().connection;
            String query = "INSERT INTO dailystatisticsdata (device_id, date, averagevalues) VALUES (?,?,?)";
            try {
                java.sql.PreparedStatement stmt = conn.prepareStatement(query);
                stmt.setString(1, differentTv);
                stmt.setString(2, deviceDataDate);
                stmt.setDouble(3, meanAvg);
                stmt.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
       
            statement = session.prepare("INSERT INTO dog.dailystatisticsdata" +
                  "(device_id, date, dailyaverage) " +
                  "VALUES (?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(differentTv,deviceDataDate,meanAvg));
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
        double meanAvg = 0;
        System.out.println(deviceRow1.count());
        if (deviceRow1.count() >0) {
            meanAvg = deviceRow1.mapToDouble(x->x).mean();
        }
        if (Double.isNaN(meanAvg)) {
            meanAvg =0;
        }
        java.sql.Connection conn = DBConnection.getInstance().connection;
             String query = "INSERT INTO dailystatisticsdata (device_id, date, averagevalues) VALUES (?,?,?)";
             try {
                java.sql.PreparedStatement stmt = conn.prepareStatement(query);
                stmt.setString(1, differentTv);
                stmt.setString(2, today);
                stmt.setDouble(3, meanAvg);
                stmt.execute();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
             
             
             
            // Inserting average temperature data for specific devices for initial run
            statement = session.prepare("INSERT INTO dog.dailystatisticsdata" +
                  "(device_id, date, dailyaverage) " +
                  "VALUES (?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(differentTv,today,meanAvg));    
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
        JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "refrigerator", mapColumnTo(Double.class))
                .select("temperature")
                .where(StatementCheck);
        deviceRow1.toArray().forEach(System.out::println);
        deviceRow1.toArray().forEach(System.out::println);
        
        for (Double cassandraRow : deviceRow1.toArray()) {
                avg += cassandraRow;
        }
        count = count + deviceRow1.count();
    
    }
   
   avg= avg/(count);
   if (Double.isNaN(avg)) {
        avg =0;
    }
   System.out.println(count);
   System.out.println(avg);
    java.sql.Connection conn = DBConnection.getInstance().connection;
    String query = "INSERT INTO dailystatisticsalldevice (device_type, date, averagevalues) VALUES (?,?,?)";
    try {
        java.sql.PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, DeviceTypes.REFRIGERATOR.toString());
        stmt.setString(2, today);
        stmt.setDouble(3, avg);
        stmt.execute();
    } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }

    // Inserting average temperature data for specific devices for initial run
    statement = session.prepare("INSERT INTO dog.dailystatisticsalldevice" +
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
        
    System.out.println(StatementCheck);
        JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "refrigerator", mapColumnTo(Double.class))
                .select("temperature")
                .where(StatementCheck);
        deviceRow1.toArray().forEach(System.out::println);      
        for (Double cassandraRow : deviceRow1.toArray()) {
                avg += cassandraRow;
        }
        count = count + deviceRow1.count();
    
    }
   avg= avg/(count);
   if (Double.isNaN(avg)) {
        avg =0;
    }
   System.out.println(count);
    System.out.println(avg);
    
    java.sql.Connection conn = DBConnection.getInstance().connection;
    String query = "INSERT INTO dailystatisticsalldevice (device_type, date, averagevalues) VALUES (?,?,?)";
    try {
        java.sql.PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, DeviceTypes.REFRIGERATOR.toString());
        stmt.setString(2, deviceDataDate);
        stmt.setDouble(3, avg);
        stmt.execute();
    } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }

   statement = session.prepare("INSERT INTO dog.dailystatisticsalldevice" +
              "(device_type, date, dailyaverageall) " +
              "VALUES (?, ?, ?);");
    boundStatement = new BoundStatement(statement);
    
       
    session.execute(boundStatement.bind(DeviceTypes.REFRIGERATOR.toString(),deviceDataDate,avg));
   }
}

}
