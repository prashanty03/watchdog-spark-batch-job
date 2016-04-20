package com.watchdog.data;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import java.time.LocalDate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class TelevisionAnalysisFunction {
	

	
	//Perform initial usage average analysis for Televisions
public static  void performInitialTelevisionUsageAnalysis(JavaRDD<String> differentTelevisionDevices,JavaRDD<String> deviceTelevisionDate, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) {
	   
    for (String differentTv : differentTelevisionDevices.toArray()) {
    	for (String deviceTelevisionDataDate : deviceTelevisionDate.toArray()) {
    	String StatementCheck = "device_id = \'" + differentTv+ "\' AND date = \'"+ deviceTelevisionDataDate +"\' ";
    	
    	System.out.println(StatementCheck);
    	JavaRDD<Integer> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "television", mapColumnTo(Integer.class))
    			.select("status")
    			.where(StatementCheck);
    	deviceRow1.toArray().forEach(System.out::println);
    	int minutesActiveTv =0;
    	double hoursActiveTv =0;
    	for (Integer cassandraRow : deviceRow1.toArray()) {
			if(cassandraRow == 1){
				minutesActiveTv+=cassandraRow; 
			}
		}
    	
    	System.out.println(minutesActiveTv);
    	System.out.println(hoursActiveTv);
    	hoursActiveTv = (double)minutesActiveTv/60;
    	System.out.println(minutesActiveTv);
   	    System.out.println(hoursActiveTv);
    	
    	
    // Inserting average usage data in hours for specific television devices initial run
    	statement = session.prepare("INSERT INTO dog.dailystatisticstelevisiondata" +
  		      "(device_id, date, dailyusage) " +
  		      "VALUES (?, ?, ?);");
  	boundStatement = new BoundStatement(statement);
  	session.execute(boundStatement.bind(differentTv,deviceTelevisionDataDate,hoursActiveTv));
	}
    }
}




//Perform daily usage analysis for Televisions
public static  void performDailyTelevisionUsageAnalysis(JavaRDD<String> differentDevices,LocalDate myDateTime, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) {
	String today = myDateTime.toString(); 
    for (String differentTv : differentDevices.toArray()) {
    	
    	String StatementCheck = " device_id = \'" + differentTv+ "\' AND date = \'"+ today +"\' ";
    	System.out.println(StatementCheck);
    	JavaRDD<Integer> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "television", mapColumnTo(Integer.class))
    			.select("status")
    			.where(StatementCheck);

    	deviceRow1.toArray().forEach(System.out::println);
    	int minutesActiveTv =0;
    	double hoursActiveTv =0;
    	for (Integer cassandraRow : deviceRow1.toArray()) {
			if(cassandraRow == 1){
				minutesActiveTv+=cassandraRow; 
			}
		}
    	
    	System.out.println(minutesActiveTv);
    	System.out.println(hoursActiveTv);
    	hoursActiveTv = (double)minutesActiveTv/60;
    	System.out.println(minutesActiveTv);
   	   System.out.println(hoursActiveTv);
    	
    	// Inserting usage in hours data for specific television devices for each day run
    	statement = session.prepare("INSERT INTO dog.dailystatisticstelevisiondata" +
  		      "(device_id, date, dailyusage) " +
  		      "VALUES (?, ?, ?);");
  	boundStatement = new BoundStatement(statement);
  	session.execute(boundStatement.bind(differentTv,today,hoursActiveTv));
	
    }
}





//Perform  daily usage analysis for all Televisions
public static  void performDailyAllelevisionUsageAnalysis(JavaRDD<String> differentDevices,LocalDate myDateTime, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) 
{
    String today = myDateTime.toString(); 
    int minutesActiveTv = 0;
    double hoursActiveTv =0;
   for (String differentTv : differentDevices.toArray()) {
        
        String StatementCheck = "device_id= \'" + differentTv+ "\'  AND date = \'"+ today +"\'";
        //System.out.println(StatementCheck1);
        JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "television", mapColumnTo(Double.class))
                .select("status")
                .where(StatementCheck);
        deviceRow1.toArray().forEach(System.out::println);
        deviceRow1.toArray().forEach(System.out::println);
        
        for (Double cassandraRow : deviceRow1.toArray()) {
            minutesActiveTv +=cassandraRow;
        }
        
    }
   
   hoursActiveTv= (double)minutesActiveTv/60;
   System.out.println(minutesActiveTv);
   System.out.println(hoursActiveTv);
   
    // Inserting average television usage for all televisions daily
    statement = session.prepare("INSERT INTO dog.dailystatisticstelevisionalldevice" +
              "(device_type, date, dailyusage) " +
              "VALUES (?, ?, ?);");
    boundStatement = new BoundStatement(statement);
     
    session.execute(boundStatement.bind(DeviceTypes.TELEVISION.toString(),today,hoursActiveTv));

}




//Perform initial usage analysis for all Televisions
public static void performInitalAllTelevisionUsgaeAnalysis(JavaRDD<String> differentTelevisionDevices,JavaRDD<String> deviceTelevisionDate, Session session, BoundStatement boundStatement, PreparedStatement statement, JavaSparkContext javaSparkContext) 
{
 for (String deviceDataDate : deviceTelevisionDate.toArray()) {
      int minutesActiveTv = 0;
    double hoursActiveTv =0;
   for (String differentTv : differentTelevisionDevices.toArray()) {
     
      String StatementCheck = "device_id= \'" + differentTv+ "\'  AND date = \'"+ deviceDataDate +"\'";
      System.out.println(StatementCheck);
      JavaRDD<Double> deviceRow1 = javaFunctions(javaSparkContext).cassandraTable("dog", "television", mapColumnTo(Double.class))
          .select("status")
          .where(StatementCheck);
      deviceRow1.toArray().forEach(System.out::println);
      //deviceRow1.toArray().forEach(System.out::println);
      
      for (Double cassandraRow : deviceRow1.toArray()) {
        minutesActiveTv +=cassandraRow;
        
      }
      
      hoursActiveTv= (double)minutesActiveTv/60;
      
         statement = session.prepare("INSERT INTO dog.dailystatisticstelevisionalldevice" +
                "(device_type, date, dailyusage) " +
                "VALUES (?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        
           
        session.execute(boundStatement.bind(DeviceTypes.TELEVISION.toString(),deviceDataDate,hoursActiveTv));
    
    }
  
   }

}


}
